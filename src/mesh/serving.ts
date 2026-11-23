import { isReported, report } from '@punyts/core';
import type { Change, RevisedChange } from '@punyts/state-sync';
import type { Access, Authority, AuthorityConnection, AuthoritySession, Handoff, RelayBatch, RelaySubscription, StoredNamespace, Upstream, WriteOutcome } from '@punyts/state-sync/authority';
import { verifyNodeCertificate } from './certificates.js';
import type { NodeCertificateClaims } from './certificates.js';
import type { LoadOutcome, MeshMessage, PersistOutcome, RemoveOutcome } from './protocol.js';

/** The category of a link's serving: node ids, addresses, prefixes and counts, never values */
const CATEGORY = 'statenet.mesh.serving';

/** Applies a write that came over a link: `via` is the node at the other end */
export type MeshWriter = (address: string, change: Change, access?: Access, via?: string) => Promise<WriteOutcome>;

/** What serves a tunnelled session: an authority, or anything that takes connections as one does */
export type SessionEndpoint = Pick<Authority, 'connect'>;

/**
 * A node reached over a link, for what it owns: the link down to a node below (`LinkClient`), or a link to a relay
 * that passes each ask on to the node (`reachThrough`)
 */
export interface RouteTarget {
    subscribe: Upstream['subscribe'];
    write(address: string, change: Change, access?: Access, from?: string): Promise<WriteOutcome>;
    open(address: string, connection: AuthorityConnection, access?: Access): AuthoritySession;
    handoff(address: string, from?: string): Promise<Handoff | undefined>;
    take(address: string): Promise<boolean>;
}

/** What a node serves to the node at the other end of a link: the same whichever end opened the link */
export interface ServingOptions {
    readonly nodeId: string;
    /** Magnetic's certificate authority, for a certificate a node asks the next one of */
    readonly authorityPublicKey: string;
    /** What this node serves subscriptions from: an owner's authorities, or its own relay */
    readonly upstream: Upstream;
    readonly write: MeshWriter;
    /**
     * The way to another node, for an ask that names one (`to`): a relay gives the node's link below it, or its link
     * to the relay the node is under (never for an ask another relay, `via`, passed on: that one is delivered below
     * or not at all, so two relays with different directories cannot pass it back and forth). Undefined when it knows none; without `route` this node passes nothing on
     */
    readonly route?: (to: string, via: NodeCertificateClaims) => RouteTarget | undefined;
    /**
     * The sessions of a namespace this node owns, for a peer that serves a view of it (an owner node of the same
     * statenet): the endpoint, or why not (it is owned elsewhere now). Without it a peer cannot open sessions here
     */
    readonly sessions?: (address: string, access: Access | undefined, peer: string) => Promise<SessionEndpoint | string>;
    /** The owner's side of a claim by the peer at the other end (`OwnerNode.handoff`) */
    readonly handoff?: (address: string, to: string) => Promise<Handoff | undefined>;
    /** The owner at the other end asks this node to claim a namespace (`OwnerNode.claim`) */
    readonly take?: (address: string) => Promise<boolean>;
    /**
     * Saves a namespace a node below sent up, and loads one: an aggregate's own, or a relay's link up to one. `node`
     * is the node that orders it. Without them this node keeps nothing for others
     */
    readonly persist?: (address: string, stored: StoredNamespace, epoch: number, node: string) => Promise<PersistOutcome>;
    readonly load?: (address: string, node: string) => Promise<LoadOutcome>;
    readonly remove?: (address: string, epoch: number, node: string) => Promise<RemoveOutcome>;
    /**
     * Answers a node's ask for a certificate's next one: `certificate` is one the authority signed and has not ended,
     * `claims` what it says, `via` the node on the link (the node itself, or a relay that passes the ask up).
     * Without it nothing is renewed here
     */
    readonly certify?: (certificate: string, claims: NodeCertificateClaims, via: NodeCertificateClaims) => Promise<string | undefined>;
    /** The most changes held back for one slow subscription before it is told to catch up instead; 500 by default */
    readonly maxHeld?: number;
    readonly now?: () => number;
}

interface Served {
    readonly subscription: RelaySubscription;
    /** Changes held back while the link has no credit, folded */
    held?: RelayBatch;
}

/** One end's serving of a link: what it does with the other end's asks */
export interface Serving {
    receive(message: MeshMessage): Promise<void>;
    /** The link closed: ends every subscription and session it served */
    close(): void;
    stats(): { readonly subscriptions: number; readonly held: number; readonly credit: number };
}

/**
 * Keeps the later of two changes at a path, and drops a change below a later set, replace or delete above it (which
 * replaces it): what a slow node needs is the state, not every step to it
 */
function fold(changes: readonly RevisedChange[]): RevisedChange[] {
    const kept: RevisedChange[] = [];
    for (const change of changes) {
        const covers = change.op !== 'merge';
        for (let index = kept.length - 1; index >= 0; index--) {
            const earlier = kept[index] as RevisedChange;
            if (earlier.path === change.path || (covers && earlier.path.startsWith(`${change.path}.`))) kept.splice(index, 1);
        }
        kept.push(change);
    }
    return kept;
}

/**
 * Serves the asks of the node at the other end of a link (`peer`): its subscriptions from `upstream`, its writes,
 * tunnelled sessions, claims, saves and certificate asks, each answer handed to `push`. A subscription's changes cost
 * one credit each: with none left they are held back and folded, and past `maxHeld` the node is told to subscribe
 * again from its revision, so a slow node lags and catches up without this node's memory growing. The server end of a
 * link serves the node that dialled; a node that dialled a relay serves the relay the same way, for what reaches it
 * from other nodes (docs/design/statenet-network.md, S9)
 */
export function createServing(options: ServingOptions, peer: NodeCertificateClaims, push: (message: MeshMessage) => void, alive: () => boolean): Serving {
    const now = options.now ?? Date.now;
    const maxHeld = options.maxHeld ?? 500;
    const subscriptions = new Map<number, Served>();
    /** The sessions tunnelled for the other end, by its tunnel id */
    const tunnels = new Map<number, AuthoritySession>();
    /** Tunnels still being opened: what arrives for them meanwhile, and whether they were shut */
    const opening = new Map<number, { said: string[]; shut: boolean }>();
    /** The writes of handoffs to the other end, waiting for their outcome, by `<cid>/<qid>` */
    const handed = new Map<string, (outcome: WriteOutcome) => void>();
    let credit = 0;
    /** Where an ask goes: null for this node itself, the way to the node it names, or undefined when there is none */
    const routed = (to: string | undefined): RouteTarget | null | undefined => (to === undefined || to === options.nodeId ? null : options.route?.(to, peer));
    /** The node an ask came from: a relay says which node it passes it on for; anything else speaks for itself */
    const origin = (from: string | undefined): string => (peer.role === 'relay' && typeof from === 'string' ? from : peer.nodeId);
    const unreachable = (to: string | undefined): string => `node ${to ?? ''} cannot be reached from ${options.nodeId}`;

    /** A subscription's changes: sent when the link has credit, held back and folded when it has none */
    const deliver = (sid: number, batch: RelayBatch): void => {
        const served = subscriptions.get(sid);
        if (served === undefined) return;
        if (served.held === undefined && credit > 0) {
            credit--;
            push({ type: 'changes', sid, batch });
            return;
        }
        const held = served.held;
        served.held = held === undefined ? batch : { previous: held.previous, revision: batch.revision, changes: fold([...held.changes, ...batch.changes]), ...(batch.epoch === undefined ? {} : { epoch: batch.epoch }) };
        if (served.held.changes.length > maxHeld) {
            // too far behind: let it go, and have the node catch up from its own revision
            report(CATEGORY, 'Node %s fell more than %s changes behind on a subscription: it catches up again', [peer.nodeId, maxHeld]);
            served.subscription.close();
            subscriptions.delete(sid);
            push({ type: 'resync', sid });
        }
    };

    const release = (): void => {
        for (const [sid, served] of subscriptions) {
            if (served.held === undefined || credit <= 0) continue;
            credit--;
            push({ type: 'changes', sid, batch: served.held });
            delete served.held;
        }
    };

    const receive = async (message: MeshMessage): Promise<void> => {
        switch (message.type) {
            case 'subscribe': {
                subscriptions.get(message.sid)?.subscription.close();
                const sid = message.sid;
                // the node below checked its own listeners' grants; this node serves it as a node
                const access: Access = { subject: peer.nodeId };
                const target = routed(message.to);
                if (target === undefined) {
                    report(CATEGORY, 'Node %s cannot pass a subscription of %s on: %s', [options.nodeId, peer.nodeId, unreachable(message.to)]);
                    push({ type: 'closed', sid, reason: unreachable(message.to) });
                    return;
                }
                if (isReported(CATEGORY)) report(CATEGORY, 'Node %s serves %s a subscription to %s %s%s', [options.nodeId, peer.nodeId, message.address, message.prefix, target === null ? '' : ` through ${message.to ?? ''}`]);
                const subscription = (target ?? options.upstream).subscribe(
                    message.address,
                    message.prefix,
                    message.revision,
                    {
                        welcome: (welcome) => {
                            const served = subscriptions.get(sid);
                            if (served !== undefined) delete served.held;
                            push({ type: 'welcome', sid, welcome });
                        },
                        changes: (batch) => deliver(sid, batch),
                        durable: (revision) => push({ type: 'durable', sid, revision }),
                        closed: (reason) => {
                            subscriptions.delete(sid);
                            push({ type: 'closed', sid, reason });
                        },
                        revoked: (entry, expires) => push({ type: 'revoked', entry, expires })
                    },
                    access
                );
                subscriptions.set(sid, { subscription });
                return;
            }
            case 'renew':
                subscriptions.get(message.sid)?.subscription.renew();
                return;
            case 'unsubscribe':
                subscriptions.get(message.sid)?.subscription.close();
                subscriptions.delete(message.sid);
                return;
            case 'credit':
                credit += Math.max(0, Math.min(message.amount, 100_000));
                release();
                return;
            case 'open': {
                const tid = message.tid;
                const pending = { said: [] as string[], shut: false };
                opening.set(tid, pending);
                const target = routed(message.to);
                const endpoint: SessionEndpoint | string =
                    target === undefined
                        ? unreachable(message.to)
                        : target !== null
                          ? { connect: (connection, access) => target.open(message.address, connection, access) }
                          : options.sessions === undefined
                            ? 'this node serves no sessions'
                            : await options.sessions(message.address, message.access, peer.nodeId).catch((error: unknown) => (error instanceof Error ? error.message : String(error)));
                opening.delete(tid);
                report(CATEGORY, 'Node %s %s a session of %s for %s%s', [options.nodeId, typeof endpoint === 'string' ? 'refused' : 'opened', message.address, peer.nodeId, typeof endpoint === 'string' ? `: ${endpoint}` : '']);
                if (typeof endpoint === 'string') {
                    if (!pending.shut) push({ type: 'shut', tid, reason: endpoint });
                    return;
                }
                if (!alive() || pending.shut) return;
                let open = true;
                const session = endpoint.connect(
                    {
                        send: (text) => {
                            if (open) push({ type: 'say', tid, text });
                        },
                        close: () => {
                            if (!open) return;
                            open = false;
                            tunnels.delete(tid);
                            push({ type: 'shut', tid, reason: 'closed' });
                        }
                    },
                    message.access
                );
                if (open) tunnels.set(tid, session);
                for (const text of pending.said) if (open) session.receive(text);
                return;
            }
            case 'say': {
                const pending = opening.get(message.tid);
                if (pending !== undefined) pending.said.push(message.text);
                else tunnels.get(message.tid)?.receive(message.text);
                return;
            }
            case 'shut': {
                const pending = opening.get(message.tid);
                if (pending !== undefined) pending.shut = true;
                const session = tunnels.get(message.tid);
                tunnels.delete(message.tid);
                session?.close();
                return;
            }
            case 'claim': {
                const target = routed(message.to);
                const handoff = target === undefined ? undefined : target !== null ? await target.handoff(message.address, origin(message.from)) : await options.handoff?.(message.address, origin(message.from)).catch(() => undefined);
                if (handoff === undefined) {
                    push({ type: 'handoff', cid: message.cid });
                    return;
                }
                const queued = handoff.queued.map((item, qid) => {
                    handed.set(`${message.cid}/${qid}`, item.done);
                    return { qid, change: item.change, ...(item.access === undefined ? {} : { access: item.access }) };
                });
                report(CATEGORY, 'Node %s passed the handoff of %s to %s at revision %s', [options.nodeId, message.address, peer.nodeId, handoff.revision]);
                push({ type: 'handoff', cid: message.cid, handoff: { revision: handoff.revision, epoch: handoff.epoch, queued } });
                return;
            }
            case 'handed': {
                const key = `${message.cid}/${message.qid}`;
                const done = handed.get(key);
                handed.delete(key);
                done?.(message.outcome);
                return;
            }
            case 'take': {
                const target = routed(message.to);
                const ok = target === undefined ? false : target !== null ? await target.take(message.address) : ((await options.take?.(message.address).catch(() => false)) ?? false);
                push({ type: 'taken', cid: message.cid, ok });
                return;
            }
            case 'persist': {
                // a relay names the node it passes the save on for; anything else speaks for itself
                const node = peer.role === 'relay' && typeof message.node === 'string' ? message.node : peer.nodeId;
                const failed = (reason: string): PersistOutcome => ({ result: 'failed', reason });
                const outcome = options.persist === undefined ? failed(`${options.nodeId} keeps nothing for other nodes`) : await options.persist(message.address, message.stored, message.epoch, node).catch((error: unknown) => failed(error instanceof Error ? error.message : String(error)));
                if (isReported(CATEGORY)) report(CATEGORY, 'Node %s saved %s for %s at revision %s: %s', [options.nodeId, message.address, node, message.stored.revision, outcome.result]);
                push({ type: 'persisted', pid: message.pid, outcome });
                return;
            }
            case 'load': {
                const node = peer.role === 'relay' && typeof message.node === 'string' ? message.node : peer.nodeId;
                const failed = (reason: string): LoadOutcome => ({ ok: false, reason });
                const outcome = options.load === undefined ? failed(`${options.nodeId} keeps nothing for other nodes`) : await options.load(message.address, node).catch((error: unknown) => failed(error instanceof Error ? error.message : String(error)));
                push({ type: 'loaded', lid: message.lid, outcome });
                return;
            }
            case 'remove': {
                const node = peer.role === 'relay' && typeof message.node === 'string' ? message.node : peer.nodeId;
                const failed = (reason: string): RemoveOutcome => ({ result: 'failed', reason });
                const outcome = options.remove === undefined ? failed(`${options.nodeId} keeps nothing for other nodes`) : await options.remove(message.address, message.epoch, node).catch((error: unknown) => failed(error instanceof Error ? error.message : String(error)));
                push({ type: 'removed', lid: message.lid, outcome });
                return;
            }
            case 'certify': {
                const checked = verifyNodeCertificate(message.certificate, options.authorityPublicKey, now());
                // a node asks for itself; only a relay passes on the ask of a node below it
                const allowed = checked.ok && (checked.claims.nodeId === peer.nodeId || peer.role === 'relay');
                const next = allowed ? await options.certify?.(message.certificate, checked.claims, peer).catch(() => undefined) : undefined;
                report('statenet.certificate', 'Node %s %s the next certificate of %s, asked by %s', [options.nodeId, next === undefined ? 'gave no' : 'passed on', checked.ok ? checked.claims.nodeId : 'an unknown node', peer.nodeId]);
                push({ type: 'certified', rid: message.rid, ...(next === undefined ? {} : { certificate: next }) });
                return;
            }
            case 'write': {
                const target = routed(message.to);
                const rejected = (reason: string): WriteOutcome => ({ result: 'rejected', reason });
                if (target === undefined) report(CATEGORY, 'Node %s cannot pass a write of %s on: %s', [options.nodeId, message.address, unreachable(message.to)]);
                const outcome = target === undefined ? rejected(unreachable(message.to)) : await (target !== null ? target.write(message.address, message.change, message.access, origin(message.from)) : options.write(message.address, message.change, message.access, origin(message.from))).catch((error: unknown): WriteOutcome => ({ result: 'rejected', reason: error instanceof Error ? error.message : String(error) }));
                if (isReported(CATEGORY)) report(CATEGORY, 'Node %s wrote %s at %s for %s: %s', [options.nodeId, message.address, message.change.path, peer.nodeId, outcome.result]);
                push({ type: 'written', wid: message.wid, outcome });
                return;
            }
            default:
                return;
        }
    };

    return {
        receive,
        close: () => {
            for (const served of subscriptions.values()) served.subscription.close();
            subscriptions.clear();
            for (const session of tunnels.values()) session.close();
            tunnels.clear();
            for (const done of handed.values()) done({ result: 'rejected', reason: 'the link to the new owner closed' });
            handed.clear();
        },
        stats: () => ({ subscriptions: subscriptions.size, held: [...subscriptions.values()].reduce((sum, served) => sum + (served.held?.changes.length ?? 0), 0), credit })
    };
}
