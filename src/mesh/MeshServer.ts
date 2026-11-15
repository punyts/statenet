import { createServer as createHttpServer } from 'node:http';
import type { Server as HttpServer } from 'node:http';
import { createServer as createHttpsServer } from 'node:https';
import type { AddressInfo } from 'node:net';
import type { Change, RevisedChange } from '@punyts/state-sync';
import type { Access, RelayBatch, RelaySubscription, Upstream, WriteOutcome } from '@punyts/state-sync/authority';
import { WebSocketServer } from 'ws';
import type { WebSocket } from 'ws';
import { checkProof, newNonce, proveNode, verifyNodeCertificate } from './certificates.js';
import type { KeyPair, NodeCertificateClaims, NodeRole } from './certificates.js';
import { MESH_PROTOCOL, parseLinkMessage } from './protocol.js';
import type { DirectoryView, LinkMessage, MeshMessage } from './protocol.js';

/** How a node takes a write from below: it orders it as the owner, or passes it up */
export type MeshWriter = (address: string, change: Change, access?: Access) => Promise<WriteOutcome>;

export interface MeshServerOptions {
    readonly nodeId: string;
    readonly role: NodeRole;
    readonly keys: KeyPair;
    readonly certificate: string;
    /** Magnetic's certificate authority: a node below must show a certificate it signed */
    readonly authorityPublicKey: string;
    /** What this node serves to the nodes below: an owner's authorities, or its own relay */
    readonly upstream: Upstream;
    readonly write: MeshWriter;
    /** The directory the nodes below learn; read when a link opens */
    readonly directory: () => DirectoryView;
    readonly host?: string;
    /** 0 for any free port */
    readonly port?: number;
    /** TLS for the links (wss): the server's key and certificate, PEM */
    readonly tls?: { readonly key: string; readonly cert: string };
    /** The most changes held back for one slow subscription before it is told to catch up instead; 500 by default */
    readonly maxHeld?: number;
    readonly now?: () => number;
}

interface Served {
    readonly subscription: RelaySubscription;
    /** Changes held back while the link has no credit, folded */
    held?: RelayBatch;
}

interface Link {
    readonly socket: WebSocket;
    readonly peer: NodeCertificateClaims;
    readonly served: Map<number, Served>;
    credit: number;
    outbox: MeshMessage[];
    scheduled: boolean;
}

/** What a server says about its links, for its owner and tests */
export interface MeshServerStats {
    readonly links: ReadonlyArray<{ readonly nodeId: string; readonly subscriptions: number; readonly held: number; readonly credit: number }>;
}

export interface MeshServer {
    readonly url: string;
    stats(): MeshServerStats;
    /** Sends a change of the directory down every link */
    directoryChanged(): void;
    close(): Promise<void>;
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
 * The parent's side of the mesh protocol (docs/design/statenet-relays.md, "The mesh protocol"): one WebSocket per node
 * below, opened with `link-hello` and its certificate, checked against Magnetic's certificate authority, and a proof of
 * the node's key each way. It then serves the node's subscriptions from `upstream` and passes its writes to `write`, in
 * frames that batch what is ready at one moment. A subscription's changes cost one credit each: with none left they are
 * held back and folded, and past `maxHeld` the node is told to subscribe again from its revision, so a slow node lags
 * and catches up without this node's memory growing
 */
export async function startMeshServer(options: MeshServerOptions): Promise<MeshServer> {
    const now = options.now ?? Date.now;
    const maxHeld = options.maxHeld ?? 500;
    const links = new Set<Link>();
    const http: HttpServer = options.tls === undefined ? createHttpServer() : (createHttpsServer({ key: options.tls.key, cert: options.tls.cert }) as unknown as HttpServer);
    const wss = new WebSocketServer({ server: http, maxPayload: 16 * 1024 * 1024 });

    const send = (socket: WebSocket, message: LinkMessage): void => {
        if (socket.readyState === socket.OPEN) socket.send(JSON.stringify(message));
    };

    const flush = (link: Link): void => {
        link.scheduled = false;
        if (link.outbox.length === 0) return;
        send(link.socket, { type: 'frame', messages: link.outbox.splice(0) });
    };

    const queue = (link: Link, message: MeshMessage): void => {
        link.outbox.push(message);
        if (!link.scheduled) {
            link.scheduled = true;
            queueMicrotask(() => flush(link));
        }
    };

    /** A subscription's changes: sent when the link has credit, held back and folded when it has none */
    const deliver = (link: Link, sid: number, batch: RelayBatch): void => {
        const served = link.served.get(sid);
        if (served === undefined) return;
        if (served.held === undefined && link.credit > 0) {
            link.credit--;
            queue(link, { type: 'changes', sid, batch });
            return;
        }
        const held = served.held;
        served.held = held === undefined ? batch : { previous: held.previous, revision: batch.revision, changes: fold([...held.changes, ...batch.changes]), ...(batch.epoch === undefined ? {} : { epoch: batch.epoch }) };
        if (served.held.changes.length > maxHeld) {
            // too far behind: let it go, and have the node catch up from its own revision
            served.subscription.close();
            link.served.delete(sid);
            queue(link, { type: 'resync', sid });
        }
    };

    const release = (link: Link): void => {
        for (const [sid, served] of link.served) {
            if (served.held === undefined || link.credit <= 0) continue;
            link.credit--;
            queue(link, { type: 'changes', sid, batch: served.held });
            delete served.held;
        }
    };

    const receive = async (link: Link, message: MeshMessage): Promise<void> => {
        switch (message.type) {
            case 'subscribe': {
                link.served.get(message.sid)?.subscription.close();
                const sid = message.sid;
                // the node below checked its own listeners' grants; this node serves it as a node
                const access: Access = { subject: link.peer.nodeId };
                const subscription = options.upstream.subscribe(
                    message.address,
                    message.prefix,
                    message.revision,
                    {
                        welcome: (welcome) => {
                            const served = link.served.get(sid);
                            if (served !== undefined) delete served.held;
                            queue(link, { type: 'welcome', sid, welcome });
                        },
                        changes: (batch) => deliver(link, sid, batch),
                        durable: (revision) => queue(link, { type: 'durable', sid, revision }),
                        closed: (reason) => {
                            link.served.delete(sid);
                            queue(link, { type: 'closed', sid, reason });
                        },
                        revoked: (grantId, expires) => queue(link, { type: 'revoked', grantId, expires })
                    },
                    access
                );
                link.served.set(sid, { subscription });
                return;
            }
            case 'renew':
                link.served.get(message.sid)?.subscription.renew();
                return;
            case 'unsubscribe':
                link.served.get(message.sid)?.subscription.close();
                link.served.delete(message.sid);
                return;
            case 'credit':
                link.credit += Math.max(0, Math.min(message.amount, 100_000));
                release(link);
                return;
            case 'write': {
                const outcome = await options.write(message.address, message.change, message.access).catch((error: unknown): WriteOutcome => ({ result: 'rejected', reason: error instanceof Error ? error.message : String(error) }));
                queue(link, { type: 'written', wid: message.wid, outcome });
                return;
            }
            default:
                return;
        }
    };

    wss.on('connection', (socket) => {
        const nonce = newNonce();
        let peer: NodeCertificateClaims | undefined;
        let link: Link | undefined;
        const refuse = (reason: string): void => {
            send(socket, { type: 'link-refused', reason });
            socket.close(4401, reason.slice(0, 100));
        };
        const timeout = setTimeout(() => {
            if (link === undefined) refuse('the link was not opened in time');
        }, 10_000);
        socket.on('message', (data) => {
            const message = parseLinkMessage(data);
            if (message === undefined) return;
            if (link !== undefined) {
                if (message.type === 'frame') for (const each of message.messages) void receive(link, each);
                else if (message.type === 'ping') send(socket, { type: 'pong' });
                return;
            }
            if (message.type === 'link-hello' && peer === undefined) {
                if (message.protocol !== MESH_PROTOCOL) return refuse(`this node speaks mesh protocol ${MESH_PROTOCOL}`);
                const checked = verifyNodeCertificate(message.certificate, options.authorityPublicKey, now());
                if (!checked.ok) return refuse(checked.reason);
                if (checked.claims.nodeId !== message.nodeId) return refuse('the certificate is for another node');
                peer = checked.claims;
                send(socket, { type: 'link-welcome', nodeId: options.nodeId, certificate: options.certificate, nonce, proof: proveNode(message.nonce, options.keys.privateKey), directory: options.directory() });
                return;
            }
            if (message.type === 'link-proof' && peer !== undefined) {
                if (!checkProof(nonce, message.proof, peer.publicKey)) return refuse('the node did not prove it holds its key');
                clearTimeout(timeout);
                link = { socket, peer, served: new Map(), credit: 0, outbox: [], scheduled: false };
                links.add(link);
                return;
            }
            refuse('open the link with link-hello and link-proof first');
        });
        socket.on('close', () => {
            clearTimeout(timeout);
            if (link === undefined) return;
            links.delete(link);
            for (const served of link.served.values()) served.subscription.close();
            link.served.clear();
        });
        socket.on('error', () => undefined);
    });

    await new Promise<void>((resolve) => http.listen(options.port ?? 0, options.host ?? '127.0.0.1', resolve));
    const address = http.address() as AddressInfo;
    const host = address.family === 'IPv6' ? `[${address.address}]` : address.address;
    return {
        url: `${options.tls === undefined ? 'ws' : 'wss'}://${host}:${address.port}`,
        stats: () => ({
            links: [...links].map((link) => ({ nodeId: link.peer.nodeId, subscriptions: link.served.size, held: [...link.served.values()].reduce((sum, served) => sum + (served.held?.changes.length ?? 0), 0), credit: link.credit }))
        }),
        directoryChanged: () => {
            const directory = options.directory();
            for (const link of links) queue(link, { type: 'directory', directory });
        },
        close: async () => {
            for (const link of links) link.socket.terminate();
            await new Promise<void>((resolve) => wss.close(() => resolve()));
            await new Promise<void>((resolve) => http.close(() => resolve()));
        }
    };
}
