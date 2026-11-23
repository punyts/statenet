import type { Change } from '@punyts/state-sync';
import type { Access, AuthorityConnection, AuthoritySession, Handoff, RelayListener, Upstream, WriteOutcome } from '@punyts/state-sync/authority';
import type { MeshMessage } from './protocol.js';

/**
 * The asking side of one open link, for the end that did not open it (docs/design/statenet-network.md, S9): a relay
 * asks the node below it for what that node owns. It lives as long as the link: when the link closes every
 * subscription is told so, every write is rejected and every session ends, and whoever asked asks again over the
 * node's next link
 */
export interface LinkClient extends Upstream {
    /** `from`: the node the write came from, when this end passes it on */
    write(address: string, change: Change, access?: Access, from?: string): Promise<WriteOutcome>;
    /** A session of a namespace the node at the other end owns, tunnelled over the link */
    open(address: string, connection: AuthorityConnection, access?: Access): AuthoritySession;
    /** Claims a namespace the node at the other end owns: its handoff, or undefined when it has none to give */
    handoff(address: string, from?: string): Promise<Handoff | undefined>;
    /** Asks the node at the other end to claim a namespace; whether it did */
    take(address: string): Promise<boolean>;
    /** An answer from the other end */
    receive(message: MeshMessage): void;
    /** The link closed */
    close(): void;
}

interface Wanted {
    readonly address: string;
    readonly prefix: string;
    readonly listener: RelayListener;
    readonly access?: Access;
    revision?: number;
}

const CLOSED = 'the link closed';

/** `push` sends an ask to the other end; `window` is how many subscription messages may be in flight (256 by default) */
export function createLinkClient(push: (message: MeshMessage) => void, options: { readonly window?: number } = {}): LinkClient {
    const window = options.window ?? 256;
    const wanted = new Map<number, Wanted>();
    const writes = new Map<number, (outcome: WriteOutcome) => void>();
    const tunnels = new Map<number, AuthorityConnection>();
    const claims = new Map<number, (handoff: Handoff | undefined) => void>();
    const takes = new Map<number, (ok: boolean) => void>();
    let next = 1;
    let taken = 0;
    let closed = false;

    const subscribeMessage = (sid: number, want: Wanted): MeshMessage => ({ type: 'subscribe', sid, address: want.address, prefix: want.prefix, ...(want.revision === undefined ? {} : { revision: want.revision }), ...(want.access === undefined ? {} : { access: want.access }) });
    /** Takes one subscription message off the link, and grants credit back in steps */
    const took = (): void => {
        taken++;
        if (taken >= window / 2) {
            push({ type: 'credit', amount: taken });
            taken = 0;
        }
    };
    push({ type: 'credit', amount: window });

    return {
        subscribe: (address, prefix, revision, listener, access) => {
            const sid = next++;
            if (closed) {
                queueMicrotask(() => listener.closed?.(CLOSED));
                return { renew: () => undefined, close: () => undefined };
            }
            const want: Wanted = { address, prefix, listener, ...(revision === undefined ? {} : { revision }), ...(access === undefined ? {} : { access }) };
            wanted.set(sid, want);
            push(subscribeMessage(sid, want));
            return {
                renew: () => {
                    if (wanted.has(sid)) push({ type: 'renew', sid });
                },
                close: () => {
                    if (wanted.delete(sid)) push({ type: 'unsubscribe', sid });
                }
            };
        },
        write: (address, change, access, from) =>
            new Promise((done) => {
                if (closed) return done({ result: 'rejected', reason: CLOSED });
                const wid = next++;
                writes.set(wid, done);
                push({ type: 'write', wid, address, change, ...(access === undefined ? {} : { access }), ...(from === undefined ? {} : { from }) });
            }),
        open: (address, connection, access) => {
            const tid = next++;
            if (closed) {
                queueMicrotask(() => connection.close());
                return { receive: () => undefined, close: () => undefined };
            }
            tunnels.set(tid, connection);
            push({ type: 'open', tid, address, ...(access === undefined ? {} : { access }) });
            return {
                receive: (text) => {
                    if (tunnels.has(tid)) push({ type: 'say', tid, text });
                },
                close: () => {
                    if (tunnels.delete(tid)) push({ type: 'shut', tid, reason: 'closed' });
                }
            };
        },
        handoff: (address, from) =>
            new Promise((done) => {
                if (closed) return done(undefined);
                const cid = next++;
                claims.set(cid, done);
                push({ type: 'claim', cid, address, ...(from === undefined ? {} : { from }) });
            }),
        take: (address) =>
            new Promise((done) => {
                if (closed) return done(false);
                const cid = next++;
                takes.set(cid, done);
                push({ type: 'take', cid, address });
            }),
        receive: (message) => {
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
                    if (want !== undefined) push(subscribeMessage(message.sid, want));
                    return;
                }
                case 'revoked':
                    for (const want of wanted.values()) want.listener.revoked?.(message.entry, message.expires);
                    return;
                case 'written': {
                    const done = writes.get(message.wid);
                    writes.delete(message.wid);
                    done?.(message.outcome);
                    return;
                }
                case 'say':
                    tunnels.get(message.tid)?.send(message.text);
                    return;
                case 'shut': {
                    const connection = tunnels.get(message.tid);
                    tunnels.delete(message.tid);
                    connection?.close();
                    return;
                }
                case 'handoff': {
                    const done = claims.get(message.cid);
                    claims.delete(message.cid);
                    const cid = message.cid;
                    done?.(
                        message.handoff === undefined
                            ? undefined
                            : {
                                  revision: message.handoff.revision,
                                  epoch: message.handoff.epoch,
                                  queued: message.handoff.queued.map(({ qid, change }) => ({ change, done: (outcome: WriteOutcome) => push({ type: 'handed', cid, qid, outcome }) }))
                              }
                    );
                    return;
                }
                case 'taken': {
                    const done = takes.get(message.cid);
                    takes.delete(message.cid);
                    done?.(message.ok);
                    return;
                }
                default:
                    return;
            }
        },
        close: () => {
            if (closed) return;
            closed = true;
            for (const want of [...wanted.values()]) want.listener.closed?.(CLOSED);
            wanted.clear();
            for (const done of [...writes.values()]) done({ result: 'rejected', reason: CLOSED });
            writes.clear();
            for (const connection of [...tunnels.values()]) connection.close();
            tunnels.clear();
            for (const done of [...claims.values()]) done(undefined);
            claims.clear();
            for (const done of [...takes.values()]) done(false);
            takes.clear();
        }
    };
}
