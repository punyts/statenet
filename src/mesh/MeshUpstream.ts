import type { Change } from '@punyts/state-sync';
import type { Access, AuthorityConnection, AuthoritySession, Handoff, RelayListener, RelaySubscription, Upstream, WriteOutcome } from '@punyts/state-sync/authority';
import WebSocket from 'ws';
import { checkProof, newNonce, proveNode, verifyNodeCertificate } from './certificates.js';
import type { KeyPair, NodeRole } from './certificates.js';
import { MESH_PROTOCOL, parseLinkMessage } from './protocol.js';
import type { DirectoryView, LinkMessage, MeshMessage } from './protocol.js';

export interface MeshUpstreamOptions {
    readonly nodeId: string;
    readonly role: NodeRole;
    readonly region?: string;
    readonly keys: KeyPair;
    readonly certificate: string;
    readonly authorityPublicKey: string;
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
    /** The revision this subscription has: the next link subscribes from it */
    revision?: number;
}

/** A node's upstream over the mesh: subscriptions and writes, kept across reconnects */
export interface MeshUpstream extends Upstream {
    write(address: string, change: Change, access?: Access): Promise<WriteOutcome>;
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
    open(address: string, connection: AuthorityConnection, access?: Access): AuthoritySession;
    /** Claims a namespace the parent owns: its handoff, or undefined when it has none to give or the link dropped */
    handoff(address: string): Promise<Handoff | undefined>;
    /** Asks the parent to claim a namespace; whether it did */
    take(address: string): Promise<boolean>;
    close(): void;
}

/**
 * A node's link up the mesh (docs/design/statenet-relays.md, "The mesh protocol"): it opens a link to its first parent
 * that answers, proves itself with its certificate and key, checks the parent's in turn, and learns the directory from
 * its `link-welcome`, so a relay needs no database credentials. Subscriptions and writes go up in frames; credits go up
 * as messages are taken. A silent or closed link is replaced by one to the next parent (from the list, then the
 * directory), and every subscription is made again from the revision it has, so nothing is missed; writes not yet
 * answered are sent again, and the owner applies each change id once
 */
export function createMeshUpstream(options: MeshUpstreamOptions): MeshUpstream {
    const now = options.now ?? Date.now;
    const window = options.window ?? 256;
    const silenceMs = options.silenceMs ?? 15_000;
    const backoff = options.backoff ?? ((attempt: number) => Math.min(10_000, 200 * 2 ** attempt));
    const wanted = new Map<number, Wanted>();
    const writes = new Map<number, { address: string; change: Change; access?: Access; done: (outcome: WriteOutcome) => void }>();
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
    const tunnels = new Map<number, { address: string; access?: Access; connection: AuthorityConnection; sent: boolean; buffer: string[] }>();
    /** Claims and takes by id, each sent once the link is open; a dropped link answers those it sent with nothing */
    const claims = new Map<number, { address: string; sent: boolean; done: (handoff: Handoff | undefined) => void }>();
    const takes = new Map<number, { address: string; sent: boolean; done: (ok: boolean) => void }>();
    const sendAsks = (): void => {
        if (!ready) return;
        for (const [cid, claim] of claims) if (!claim.sent) {
            claim.sent = true;
            queue({ type: 'claim', cid, address: claim.address });
        }
        for (const [cid, take] of takes) if (!take.sent) {
            take.sent = true;
            queue({ type: 'take', cid, address: take.address });
        }
    };
    let nextTid = 1;
    let nextCid = 1;
    const openTunnel = (tid: number): void => {
        const tunnel = tunnels.get(tid);
        if (tunnel === undefined || tunnel.sent || !ready) return;
        tunnel.sent = true;
        queue({ type: 'open', tid, address: tunnel.address, ...(tunnel.access === undefined ? {} : { access: tunnel.access }) });
        for (const text of tunnel.buffer.splice(0)) queue({ type: 'say', tid, text });
    };

    const send = (message: LinkMessage): void => {
        if (socket !== undefined && socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(message));
    };
    const queue = (message: MeshMessage): void => {
        if (!ready) return;
        outbox.push(message);
        if (!scheduled) {
            scheduled = true;
            queueMicrotask(() => {
                scheduled = false;
                if (ready && outbox.length > 0) send({ type: 'frame', messages: outbox.splice(0) });
            });
        }
    };

    /** The parents to try, in order: the configured ones, then any other node the directory names */
    const candidates = (): string[] => [...new Set([...options.parents, ...(options.followDirectory === false ? [] : directory?.nodes ?? []).filter((node) => node.endpoint !== '' && node.id !== options.nodeId).map((node) => node.endpoint)])];

    const subscribeMessage = (sid: number, want: Wanted): MeshMessage => ({ type: 'subscribe', sid, address: want.address, prefix: want.prefix, ...(want.revision === undefined ? {} : { revision: want.revision }), ...(want.access === undefined ? {} : { access: want.access }) });

    const opened = (): void => {
        ready = true;
        attempt = 0;
        if (options.manualCredit !== true) queue({ type: 'credit', amount: window });
        for (const [sid, want] of wanted) queue(subscribeMessage(sid, want));
        for (const [wid, write] of writes) queue({ type: 'write', wid, address: write.address, change: write.change, ...(write.access === undefined ? {} : { access: write.access }) });
        for (const tid of tunnels.keys()) openTunnel(tid);
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
        const link = new WebSocket(url);
        socket = link;
        link.on('open', () => {
            send({ type: 'link-hello', nodeId: options.nodeId, role: options.role, protocol: MESH_PROTOCOL, region: options.region ?? '', certificate: options.certificate, nonce });
        });
        link.on('message', (data) => {
            if (link !== socket) return;
            lastHeard = now();
            const message = parseLinkMessage(data);
            if (message === undefined) return;
            if (message.type === 'link-welcome' && !ready) {
                const checked = verifyNodeCertificate(message.certificate, options.authorityPublicKey, now());
                if (!checked.ok || checked.claims.nodeId !== message.nodeId || !checkProof(nonce, message.proof, checked.claims.publicKey)) {
                    options.report?.(`Refused the parent at ${url}: ${checked.ok ? 'it did not prove it holds its key' : checked.reason}`);
                    link.terminate();
                    return;
                }
                directory = message.directory;
                options.onDirectory?.(message.directory);
                send({ type: 'link-proof', proof: proveNode(message.nonce, options.keys.privateKey) });
                current = url;
                opened();
                ping = setInterval(() => {
                    if (now() - lastHeard > silenceMs) link.terminate();
                    else send({ type: 'ping' });
                }, Math.max(20, silenceMs / 3));
                (ping as { unref?: () => void }).unref?.();
                return;
            }
            if (message.type === 'link-refused') {
                options.report?.(`The parent at ${url} refused the link: ${message.reason}`);
                return;
            }
            if (message.type === 'frame' && ready) for (const each of message.messages) receive(each);
        });
        link.on('close', () => {
            if (link === socket) dropped();
        });
        link.on('error', () => undefined);
    }

    connect();

    return {
        subscribe: (address, prefix, revision, listener, access): RelaySubscription => {
            const sid = nextSid++;
            const want: Wanted = { address, prefix, listener, ...(revision === undefined ? {} : { revision }), ...(access === undefined ? {} : { access }) };
            wanted.set(sid, want);
            queue(subscribeMessage(sid, want));
            return {
                renew: () => queue({ type: 'renew', sid }),
                close: () => {
                    if (wanted.delete(sid)) queue({ type: 'unsubscribe', sid });
                }
            };
        },
        write: (address, change, access) =>
            new Promise((done) => {
                const wid = nextWid++;
                writes.set(wid, { address, change, ...(access === undefined ? {} : { access }), done });
                queue({ type: 'write', wid, address, change, ...(access === undefined ? {} : { access }) });
            }),
        directory: () => directory,
        linked: () => ready,
        parent: () => current,
        grant: (amount) => queue({ type: 'credit', amount }),
        open: (address, connection, access) => {
            const tid = nextTid++;
            tunnels.set(tid, { address, connection, sent: false, buffer: [], ...(access === undefined ? {} : { access }) });
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
        handoff: (address) =>
            new Promise((done) => {
                if (closed) return done(undefined);
                claims.set(nextCid++, { address, sent: false, done });
                sendAsks();
            }),
        take: (address) =>
            new Promise((done) => {
                if (closed) return done(false);
                takes.set(nextCid++, { address, sent: false, done });
                sendAsks();
            }),
        close: () => {
            closed = true;
            for (const tunnel of tunnels.values()) tunnel.connection.close();
            tunnels.clear();
            for (const claim of claims.values()) claim.done(undefined);
            claims.clear();
            for (const take of takes.values()) take.done(false);
            takes.clear();
            if (timer !== undefined) clearTimeout(timer);
            if (ping !== undefined) clearInterval(ping);
            socket?.terminate();
            ready = false;
        }
    };
}
