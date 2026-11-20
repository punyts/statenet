import type { Change } from '@punyts/state-sync';
import type { Access, AuthorityConnection, AuthoritySession, Handoff, RelayListener, RelaySubscription, StoredNamespace, Upstream, WriteOutcome } from '@punyts/state-sync/authority';
import WebSocket from 'ws';
import { checkProof, mayLink, newNonce, proveNode, verifyNodeCertificate } from './certificates.js';
import { newShare, sealOf, transcript } from './sealing.js';
import type { LinkSeal } from './sealing.js';
import { createServing } from './serving.js';
import type { RouteTarget, Serving, ServingOptions } from './serving.js';
import type { NodeCertificateClaims } from './certificates.js';
import type { KeyPair, NodeRole } from './certificates.js';
import { MESH_PROTOCOL, parseLinkMessage } from './protocol.js';
import type { DirectoryView, LinkMessage, LoadOutcome, MeshMessage, PersistOutcome, RemoveOutcome } from './protocol.js';

export interface MeshUpstreamOptions {
    readonly nodeId: string;
    readonly role: NodeRole;
    /** The group of a node (`owner`): it links to a node beside it only when that node is of the same group */
    readonly group?: string;
    /**
     * What this node serves to its parent, for what reaches the parent from other nodes: a node linked to a relay
     * gives what its own mesh server gives (docs/design/statenet-network.md, S9). Without it the parent's asks are
     * answered as unserved
     */
    readonly serve?: Omit<ServingOptions, 'nodeId' | 'authorityPublicKey'>;
    readonly region?: string;
    readonly keys: KeyPair;
    /**
     * This node's certificate. Given as text, the node asks its parent for the next one before it ends
     * (`renewWithinMs`, `onCertificate`); given as a function, whoever gives it keeps it fresh
     */
    readonly certificate: string | (() => string);
    readonly authorityPublicKey: string;
    /** How long before its certificate ends the node asks for the next one; a day by default */
    readonly renewWithinMs?: number;
    /** The node's next certificate, when its parent gave one: keep it for the next start */
    readonly onCertificate?: (certificate: string) => void;
    /** The parents to link to, in order of preference; more are learned from the directory */
    readonly parents: readonly string[];
    /** Whether to try the nodes the directory names when the parents do not answer; true by default (a peer link to one node: false) */
    readonly followDirectory?: boolean;
    /** Credits granted at a time: how many subscription messages may be in flight; 256 by default */
    readonly window?: number;
    /** Grants no credit by itself (tests of a slow node): call `grant` */
    readonly manualCredit?: boolean;
    /** The wait before reconnect attempt n; 200 ms doubling to 10 s by default */
    readonly backoff?: (attempt: number) => number;
    /** How long a link may be silent before it is closed; 15 seconds by default (pings every third of it) */
    readonly silenceMs?: number;
    readonly onDirectory?: (directory: DirectoryView) => void;
    readonly report?: (message: string) => void;
    readonly now?: () => number;
}

interface Wanted {
    readonly address: string;
    readonly prefix: string;
    readonly listener: RelayListener;
    readonly access?: Access;
    /** The node the subscription is for, when the parent is to pass it on */
    readonly to?: string;
    /** The revision this subscription has: the next link subscribes from it */
    revision?: number;
}

/** A node's upstream over the mesh: subscriptions and writes, kept across reconnects */
export interface MeshUpstream extends Upstream {
    /** `to`: the node that owns the namespace, when the parent is a relay that passes the subscription on to it */
    subscribe(address: string, prefix: string, revision: number | undefined, listener: RelayListener, access?: Access, to?: string): RelaySubscription;
    /** `to`: the node the write is for, when the parent passes it on; `from`: the node it came from, when this node does */
    write(address: string, change: Change, access?: Access, to?: string, from?: string): Promise<WriteOutcome>;
    /** The id of the parent it is linked to */
    parentId(): string | undefined;
    /** The directory as the parent last told it */
    directory(): DirectoryView | undefined;
    /** Whether a link to a parent is open */
    linked(): boolean;
    /** The parent it is linked to */
    parent(): string | undefined;
    /** Grants credits by hand (with `manualCredit`) */
    grant(amount: number): void;
    /**
     * A session of a namespace the parent owns, tunnelled over the link for a view this node serves: what the session
     * is given goes to the parent's authority, what the authority sends comes to `connection`. A dropped link closes it
     */
    open(address: string, connection: AuthorityConnection, access?: Access, to?: string): AuthoritySession;
    /** Claims a namespace the parent (or the node `to`, through it) owns: its handoff, or undefined when it has none to give or the link dropped */
    handoff(address: string, to?: string, from?: string): Promise<Handoff | undefined>;
    /** Asks the parent (or the node `to`, through it) to claim a namespace; whether it did */
    take(address: string, to?: string): Promise<boolean>;
    /** The certificate this node shows now */
    certificate(): string;
    /**
     * Sends a namespace up to be saved, and answers when the aggregate did (docs/design/statenet-network.md). A save
     * not yet answered when the link drops is sent again on the next one; the aggregate writes nothing twice
     */
    persist(address: string, stored: StoredNamespace, epoch: number, node?: string): Promise<PersistOutcome>;
    /** Loads a namespace from the aggregate; asked again on the next link when this one drops */
    load(address: string, node?: string): Promise<LoadOutcome>;
    /** Has the aggregate remove a namespace whole, under the epoch this node holds it under; asked again on the next link */
    remove(address: string, epoch: number, node?: string): Promise<RemoveOutcome>;
    /** Asks the parent for a certificate's next one (this node's, or one a node below asked for); undefined when it gives none */
    certify(certificate: string): Promise<string | undefined>;
    close(): void;
}

/** A node as it is reached through a link to a relay: every ask names the node, and the relay passes it on */
export const reachThrough = (link: MeshUpstream, to: string): RouteTarget => ({
    subscribe: (address, prefix, revision, listener, access) => link.subscribe(address, prefix, revision, listener, access, to),
    write: (address, change, access, from) => link.write(address, change, access, to, from),
    open: (address, connection, access) => link.open(address, connection, access, to),
    handoff: (address, from) => link.handoff(address, to, from),
    take: (address) => link.take(address, to)
});

/**
 * A node's link up the mesh (docs/design/statenet-relays.md, "The mesh protocol"): it opens a link to its first parent
 * that answers, proves itself with its certificate and key, checks the parent's in turn, and learns the directory from
 * its `link-welcome`, so a relay needs no database credentials. Subscriptions and writes go up in frames; credits go up
 * as messages are taken. Everything after the handshake is sealed with the key the two ends agreed in it (`sealOf`), and
 * a parent of a role this node does not link to is refused (`mayLink`). A silent or closed link is replaced by one to the next parent (from the list, then the
 * directory), and every subscription is made again from the revision it has, so nothing is missed; writes not yet
 * answered are sent again, and the owner applies each change id once
 */
export function createMeshUpstream(options: MeshUpstreamOptions): MeshUpstream {
    const now = options.now ?? Date.now;
    const window = options.window ?? 256;
    const silenceMs = options.silenceMs ?? 15_000;
    const backoff = options.backoff ?? ((attempt: number) => Math.min(10_000, 200 * 2 ** attempt));
    const wanted = new Map<number, Wanted>();
    const writes = new Map<number, { address: string; change: Change; access?: Access; to?: string; from?: string; done: (outcome: WriteOutcome) => void }>();
    const writeMessage = (wid: number, write: { address: string; change: Change; access?: Access; to?: string; from?: string }): MeshMessage => ({ type: 'write', wid, address: write.address, change: write.change, ...(write.access === undefined ? {} : { access: write.access }), ...(write.to === undefined ? {} : { to: write.to }), ...(write.from === undefined ? {} : { from: write.from }) });
    /** The parent's certificate, once it showed it, and what this node serves it while the link is open */
    let parent: NodeCertificateClaims | undefined;
    let serving: Serving | undefined;
    const unserved: Omit<ServingOptions, 'nodeId' | 'authorityPublicKey'> = {
        upstream: {
            subscribe: (_address, _prefix, _revision, listener) => {
                queueMicrotask(() => listener.closed?.(`${options.nodeId} serves nothing to its parent`));
                return { renew: () => undefined, close: () => undefined };
            }
        },
        write: async () => ({ result: 'rejected', reason: `${options.nodeId} serves nothing to its parent` })
    };
    let nextSid = 1;
    let nextWid = 1;
    let socket: WebSocket | undefined;
    let ready = false;
    let current: string | undefined;
    let directory: DirectoryView | undefined;
    let attempt = 0;
    let closed = false;
    let taken = 0;
    let lastHeard = now();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let ping: ReturnType<typeof setInterval> | undefined;
    const outbox: MeshMessage[] = [];
    let scheduled = false;
    /** Tunnelled sessions by id; `sent` once the parent was told to open it, `buffer` what waits until then */
    const tunnels = new Map<number, { address: string; access?: Access; to?: string; connection: AuthorityConnection; sent: boolean; buffer: string[] }>();
    /** Claims and takes by id, each sent once the link is open; a dropped link answers those it sent with nothing */
    const claims = new Map<number, { address: string; to?: string; from?: string; sent: boolean; done: (handoff: Handoff | undefined) => void }>();
    const takes = new Map<number, { address: string; to?: string; sent: boolean; done: (ok: boolean) => void }>();
    const certifies = new Map<number, { certificate: string; sent: boolean; done: (certificate: string | undefined) => void }>();
    /** Saves and loads not yet answered: kept across links, like writes */
    const persists = new Map<number, { address: string; stored: StoredNamespace; epoch: number; node?: string; done: (outcome: PersistOutcome) => void }>();
    const loads = new Map<number, { address: string; node?: string; done: (outcome: LoadOutcome) => void }>();
    const persistMessage = (pid: number, save: { address: string; stored: StoredNamespace; epoch: number; node?: string }): MeshMessage => ({ type: 'persist', pid, address: save.address, epoch: save.epoch, stored: save.stored, ...(save.node === undefined ? {} : { node: save.node }) });
    const removals = new Map<number, { address: string; epoch: number; node?: string; done: (outcome: RemoveOutcome) => void }>();
    const removeMessage = (lid: number, removal: { address: string; epoch: number; node?: string }): MeshMessage => ({ type: 'remove', lid, address: removal.address, epoch: removal.epoch, ...(removal.node === undefined ? {} : { node: removal.node }) });
    const loadMessage = (lid: number, load: { address: string; node?: string }): MeshMessage => ({ type: 'load', lid, address: load.address, ...(load.node === undefined ? {} : { node: load.node }) });
    /** What seals the open link's messages; undefined until its handshake is through */
    let seal: LinkSeal | undefined;
    /** The certificate as text, when this node renews it over its link */
    let held = typeof options.certificate === 'string' ? options.certificate : undefined;
    const certificateNow = (): string => held ?? (options.certificate as () => string)();
    const renewWithinMs = options.renewWithinMs ?? 24 * 3_600_000;
    let nextRenewal = 0;
    const sendAsks = (): void => {
        if (!ready) return;
        for (const [rid, ask] of certifies) if (!ask.sent) {
            ask.sent = true;
            queue({ type: 'certify', rid, certificate: ask.certificate });
        }
        for (const [cid, claim] of claims) if (!claim.sent) {
            claim.sent = true;
            queue({ type: 'claim', cid, address: claim.address, ...(claim.to === undefined ? {} : { to: claim.to }), ...(claim.from === undefined ? {} : { from: claim.from }) });
        }
        for (const [cid, take] of takes) if (!take.sent) {
            take.sent = true;
            queue({ type: 'take', cid, address: take.address, ...(take.to === undefined ? {} : { to: take.to }) });
        }
    };
    let nextTid = 1;
    let nextCid = 1;
    const openTunnel = (tid: number): void => {
        const tunnel = tunnels.get(tid);
        if (tunnel === undefined || tunnel.sent || !ready) return;
        tunnel.sent = true;
        queue({ type: 'open', tid, address: tunnel.address, ...(tunnel.access === undefined ? {} : { access: tunnel.access }), ...(tunnel.to === undefined ? {} : { to: tunnel.to }) });
        for (const text of tunnel.buffer.splice(0)) queue({ type: 'say', tid, text });
    };

    /** A message of the handshake, in the clear */
    const send = (message: LinkMessage): void => {
        if (socket !== undefined && socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(message));
    };
    /** A message of the open link, sealed */
    const sendSealed = (message: LinkMessage): void => {
        if (socket !== undefined && socket.readyState === WebSocket.OPEN && seal !== undefined) socket.send(seal.seal(JSON.stringify(message)));
    };
    const queue = (message: MeshMessage): void => {
        if (!ready) return;
        outbox.push(message);
        if (!scheduled) {
            scheduled = true;
            queueMicrotask(() => {
                scheduled = false;
                if (ready && outbox.length > 0) sendSealed({ type: 'frame', messages: outbox.splice(0) });
            });
        }
    };

    const certify = (certificate: string): Promise<string | undefined> =>
        new Promise((done) => {
            if (closed) return done(undefined);
            certifies.set(nextCid++, { certificate, sent: false, done });
            sendAsks();
        });

    /** Asks for this node's next certificate when the one it holds ends soon; a refusal is asked again a minute later */
    const renew = (): void => {
        if (held === undefined || !ready || now() < nextRenewal) return;
        const mine = verifyNodeCertificate(held, options.authorityPublicKey, now());
        if (!mine.ok || mine.claims.expires - now() > renewWithinMs) return;
        nextRenewal = now() + 60_000;
        void certify(held).then((next) => {
            const checked = verifyNodeCertificate(next, options.authorityPublicKey, now());
            if (next === undefined || !checked.ok || checked.claims.nodeId !== options.nodeId || checked.claims.role !== options.role || checked.claims.publicKey !== options.keys.publicKey) return;
            if (checked.claims.expires <= mine.claims.expires) return;
            held = next;
            options.onCertificate?.(next);
        });
    };

    /** The parents to try, in order: the configured ones, then any other node the directory names */
    const candidates = (): string[] => [...new Set([...options.parents, ...(options.followDirectory === false ? [] : directory?.nodes ?? []).filter((node) => node.endpoint !== '' && node.id !== options.nodeId).map((node) => node.endpoint)])];

    const subscribeMessage = (sid: number, want: Wanted): MeshMessage => ({ type: 'subscribe', sid, address: want.address, prefix: want.prefix, ...(want.revision === undefined ? {} : { revision: want.revision }), ...(want.access === undefined ? {} : { access: want.access }), ...(want.to === undefined ? {} : { to: want.to }) });

    const opened = (): void => {
        ready = true;
        attempt = 0;
        if (options.manualCredit !== true) queue({ type: 'credit', amount: window });
        for (const [sid, want] of wanted) queue(subscribeMessage(sid, want));
        for (const [wid, write] of writes) queue(writeMessage(wid, write));
        for (const tid of tunnels.keys()) openTunnel(tid);
        for (const [pid, save] of persists) queue(persistMessage(pid, save));
        for (const [lid, load] of loads) queue(loadMessage(lid, load));
        for (const [lid, removal] of removals) queue(removeMessage(lid, removal));
        sendAsks();
    };

    /** Takes one subscription message off the link, and grants credit back in steps */
    const took = (): void => {
        if (options.manualCredit === true) return;
        taken++;
        if (taken >= window / 2) {
            queue({ type: 'credit', amount: taken });
            taken = 0;
        }
    };

    const receive = (message: MeshMessage): void => {
        switch (message.type) {
            case 'welcome': {
                const want = wanted.get(message.sid);
                if (want === undefined) return;
                want.revision = message.welcome.revision;
                took();
                want.listener.welcome(message.welcome);
                return;
            }
            case 'changes': {
                const want = wanted.get(message.sid);
                if (want === undefined) return;
                want.revision = message.batch.revision;
                took();
                want.listener.changes(message.batch);
                return;
            }
            case 'durable':
                wanted.get(message.sid)?.listener.durable?.(message.revision);
                return;
            case 'closed': {
                const want = wanted.get(message.sid);
                wanted.delete(message.sid);
                want?.listener.closed?.(message.reason);
                return;
            }
            case 'resync': {
                const want = wanted.get(message.sid);
                if (want !== undefined) queue(subscribeMessage(message.sid, want));
                return;
            }
            case 'revoked':
                for (const want of wanted.values()) want.listener.revoked?.(message.grantId, message.expires);
                return;
            case 'written': {
                const write = writes.get(message.wid);
                writes.delete(message.wid);
                write?.done(message.outcome);
                return;
            }
            case 'persisted': {
                const done = persists.get(message.pid)?.done;
                persists.delete(message.pid);
                done?.(message.outcome);
                return;
            }
            case 'loaded': {
                const done = loads.get(message.lid)?.done;
                loads.delete(message.lid);
                done?.(message.outcome);
                return;
            }
            case 'removed': {
                const done = removals.get(message.lid)?.done;
                removals.delete(message.lid);
                done?.(message.outcome);
                return;
            }
            case 'certified': {
                const done = certifies.get(message.rid)?.done;
                certifies.delete(message.rid);
                done?.(message.certificate);
                return;
            }
            case 'directory':
                directory = message.directory;
                options.onDirectory?.(message.directory);
                return;
            case 'say':
                tunnels.get(message.tid)?.connection.send(message.text);
                return;
            case 'shut': {
                const tunnel = tunnels.get(message.tid);
                tunnels.delete(message.tid);
                tunnel?.connection.close();
                return;
            }
            case 'handoff': {
                const done = claims.get(message.cid)?.done;
                claims.delete(message.cid);
                const cid = message.cid;
                done?.(
                    message.handoff === undefined
                        ? undefined
                        : {
                              revision: message.handoff.revision,
                              epoch: message.handoff.epoch,
                              queued: message.handoff.queued.map(({ qid, change }) => ({ change, done: (outcome: WriteOutcome) => queue({ type: 'handed', cid, qid, outcome }) }))
                          }
                );
                return;
            }
            case 'taken': {
                const done = takes.get(message.cid)?.done;
                takes.delete(message.cid);
                done?.(message.ok);
                return;
            }
            default:
                return;
        }
    };

    const dropped = (): void => {
        ready = false;
        socket = undefined;
        seal = undefined;
        parent = undefined;
        serving?.close();
        serving = undefined;
        current = undefined;
        if (ping !== undefined) clearInterval(ping);
        ping = undefined;
        // what was open over the link ends with it: the views reconnect, and the claims are answered with nothing
        for (const [tid, tunnel] of [...tunnels]) {
            if (!tunnel.sent) continue;
            tunnels.delete(tid);
            tunnel.connection.close();
        }
        for (const [cid, claim] of [...claims]) if (claim.sent) {
            claims.delete(cid);
            claim.done(undefined);
        }
        for (const [cid, take] of [...takes]) if (take.sent) {
            takes.delete(cid);
            take.done(false);
        }
        for (const [rid, ask] of [...certifies]) if (ask.sent) {
            certifies.delete(rid);
            ask.done(undefined);
        }
        if (closed) return;
        timer = setTimeout(connect, backoff(attempt++));
        (timer as { unref?: () => void }).unref?.();
    };

    function connect(): void {
        timer = undefined;
        if (closed) return;
        const list = candidates();
        const url = list[attempt % Math.max(1, list.length)];
        if (url === undefined) return;
        const nonce = newNonce();
        const mine = newShare();
        const link = new WebSocket(url);
        socket = link;
        seal = undefined;
        const refuse = (why: string): void => {
            options.report?.(`Refused the parent at ${url}: ${why}`);
            link.terminate();
        };
        // a parent that takes the socket and then says nothing is left for the next one
        const handshake = setTimeout(() => {
            if (link === socket && !ready) link.terminate();
        }, Math.max(1_000, silenceMs));
        (handshake as { unref?: () => void }).unref?.();
        link.on('open', () => {
            send({ type: 'link-hello', nodeId: options.nodeId, role: options.role, protocol: MESH_PROTOCOL, region: options.region ?? '', certificate: certificateNow(), nonce, share: mine.share });
        });
        link.on('message', (data, isBinary) => {
            if (link !== socket) return;
            lastHeard = now();
            if (seal !== undefined) {
                // past the handshake every message is sealed: one that does not open was changed, replayed or lost one before it
                const text = isBinary ? seal.open(data) : undefined;
                const sealed = text === undefined ? undefined : parseLinkMessage(text);
                if (sealed === undefined) return refuse('a message on the link did not open');
                if (sealed.type !== 'frame') return;
                let messages = sealed.messages;
                if (!ready) {
                    // the parent's first frame starts with the directory: it let this node in
                    const first = messages[0];
                    if (first?.type !== 'directory') return refuse('it did not open the link with the directory');
                    messages = messages.slice(1);
                    directory = first.directory;
                    options.onDirectory?.(first.directory);
                    current = url;
                    options.report?.(`Linked to the parent at ${url}`);
                    // what the parent asks of this node comes as `rev`, and is answered the same way
                    if (parent !== undefined) serving = createServing({ nodeId: options.nodeId, authorityPublicKey: options.authorityPublicKey, ...(options.serve ?? unserved) }, parent, (answer) => queue({ type: 'rev', message: answer }), () => ready && link === socket);
                    opened();
                    ping = setInterval(() => {
                        if (now() - lastHeard > silenceMs) link.terminate();
                        else {
                            sendSealed({ type: 'ping' });
                            renew();
                        }
                    }, Math.max(20, silenceMs / 3));
                    (ping as { unref?: () => void }).unref?.();
                    renew();
                }
                for (const each of messages) {
                    if (each.type === 'rev') void serving?.receive(each.message);
                    else receive(each);
                }
                return;
            }
            const message = isBinary ? undefined : parseLinkMessage(data);
            if (message === undefined) return;
            if (message.type === 'link-welcome') {
                const checked = verifyNodeCertificate(message.certificate, options.authorityPublicKey, now());
                if (!checked.ok) return refuse(checked.reason);
                if (checked.claims.nodeId !== message.nodeId) return refuse('its certificate is for another node');
                if (!mayLink(options.role, checked.claims.role)) return refuse(`a node of role ${options.role} does not link to one of role ${checked.claims.role}`);
                if (options.role === 'owner' && checked.claims.role === 'owner' && (checked.claims.group ?? '') !== (options.group ?? '')) return refuse(`a node of group ${options.group ?? '(none)'} does not link to a node of group ${checked.claims.group ?? '(none)'}`);
                if (typeof message.nonce !== 'string' || typeof message.share !== 'string') return refuse('its welcome has no challenge or no share');
                const facts = { diallerShare: mine.share, serverShare: message.share, dialler: options.nodeId, server: checked.claims.nodeId };
                if (!checkProof(transcript('server', nonce, facts), message.proof, checked.claims.publicKey)) return refuse('it did not prove it holds its key');
                const sealed = sealOf(mine, message.share, { dialler: nonce, server: message.nonce }, 'dialler');
                if (sealed === undefined) return refuse('its share is not a key');
                send({ type: 'link-proof', proof: proveNode(transcript('dialler', message.nonce, facts), options.keys.privateKey) });
                seal = sealed;
                parent = checked.claims;
                return;
            }
            if (message.type === 'link-refused') options.report?.(`The parent at ${url} refused the link: ${message.reason}`);
        });
        link.on('close', () => {
            clearTimeout(handshake);
            if (link === socket) dropped();
        });
        link.on('error', () => undefined);
    }

    connect();

    return {
        subscribe: (address, prefix, revision, listener, access, to): RelaySubscription => {
            const sid = nextSid++;
            const want: Wanted = { address, prefix, listener, ...(revision === undefined ? {} : { revision }), ...(access === undefined ? {} : { access }), ...(to === undefined ? {} : { to }) };
            wanted.set(sid, want);
            queue(subscribeMessage(sid, want));
            return {
                renew: () => queue({ type: 'renew', sid }),
                close: () => {
                    if (wanted.delete(sid)) queue({ type: 'unsubscribe', sid });
                }
            };
        },
        write: (address, change, access, to, from) =>
            new Promise((done) => {
                const wid = nextWid++;
                const write = { address, change, ...(access === undefined ? {} : { access }), ...(to === undefined ? {} : { to }), ...(from === undefined ? {} : { from }), done };
                writes.set(wid, write);
                queue(writeMessage(wid, write));
            }),
        parentId: () => (ready ? parent?.nodeId : undefined),
        directory: () => directory,
        linked: () => ready,
        parent: () => current,
        grant: (amount) => queue({ type: 'credit', amount }),
        open: (address, connection, access, to) => {
            const tid = nextTid++;
            tunnels.set(tid, { address, connection, sent: false, buffer: [], ...(access === undefined ? {} : { access }), ...(to === undefined ? {} : { to }) });
            openTunnel(tid);
            return {
                receive: (text) => {
                    const tunnel = tunnels.get(tid);
                    if (tunnel === undefined) return;
                    if (tunnel.sent) queue({ type: 'say', tid, text });
                    else tunnel.buffer.push(text);
                },
                close: () => {
                    const tunnel = tunnels.get(tid);
                    tunnels.delete(tid);
                    if (tunnel?.sent === true) queue({ type: 'shut', tid, reason: 'closed' });
                }
            };
        },
        handoff: (address, to, from) =>
            new Promise((done) => {
                if (closed) return done(undefined);
                claims.set(nextCid++, { address, sent: false, done, ...(to === undefined ? {} : { to }), ...(from === undefined ? {} : { from }) });
                sendAsks();
            }),
        take: (address, to) =>
            new Promise((done) => {
                if (closed) return done(false);
                takes.set(nextCid++, { address, sent: false, done, ...(to === undefined ? {} : { to }) });
                sendAsks();
            }),
        certificate: certificateNow,
        certify,
        persist: (address, stored, epoch, node) =>
            new Promise((done) => {
                if (closed) return done({ result: 'failed', reason: 'the link is closed' });
                const pid = nextCid++;
                const save = { address, stored, epoch, ...(node === undefined ? {} : { node }), done };
                persists.set(pid, save);
                queue(persistMessage(pid, save));
            }),
        remove: (address, epoch, node) =>
            new Promise((done) => {
                if (closed) return done({ result: 'failed', reason: 'the link is closed' });
                const lid = nextCid++;
                const removal = { address, epoch, ...(node === undefined ? {} : { node }), done };
                removals.set(lid, removal);
                queue(removeMessage(lid, removal));
            }),
        load: (address, node) =>
            new Promise((done) => {
                if (closed) return done({ ok: false, reason: 'the link is closed' });
                const lid = nextCid++;
                const load = { address, ...(node === undefined ? {} : { node }), done };
                loads.set(lid, load);
                queue(loadMessage(lid, load));
            }),
        close: () => {
            closed = true;
            for (const tunnel of tunnels.values()) tunnel.connection.close();
            tunnels.clear();
            for (const claim of claims.values()) claim.done(undefined);
            claims.clear();
            for (const take of takes.values()) take.done(false);
            takes.clear();
            for (const ask of certifies.values()) ask.done(undefined);
            certifies.clear();
            for (const save of persists.values()) save.done({ result: 'failed', reason: 'the link is closed' });
            persists.clear();
            for (const load of loads.values()) load.done({ ok: false, reason: 'the link is closed' });
            loads.clear();
            for (const removal of removals.values()) removal.done({ result: 'failed', reason: 'the link is closed' });
            removals.clear();
            if (timer !== undefined) clearTimeout(timer);
            if (ping !== undefined) clearInterval(ping);
            socket?.terminate();
            ready = false;
        }
    };
}
