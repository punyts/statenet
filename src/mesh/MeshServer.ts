import { createServer as createHttpServer } from 'node:http';
import type { Server as HttpServer } from 'node:http';
import { createServer as createHttpsServer } from 'node:https';
import type { AddressInfo } from 'node:net';
import type { Change, RevisedChange } from '@punyts/state-sync';
import type { Access, Authority, AuthoritySession, Handoff, RelayBatch, RelaySubscription, StoredNamespace, Upstream, WriteOutcome } from '@punyts/state-sync/authority';
import { WebSocketServer } from 'ws';
import type { WebSocket } from 'ws';
import { checkProof, mayLink, newNonce, proveNode, verifyNodeCertificate } from './certificates.js';
import { newShare, sealOf, transcript } from './sealing.js';
import type { LinkSeal } from './sealing.js';
import type { KeyPair, NodeCertificateClaims, NodeRole } from './certificates.js';
import { MESH_PROTOCOL, parseLinkMessage } from './protocol.js';
import type { DirectoryView, LinkMessage, LoadOutcome, MeshMessage, PersistOutcome } from './protocol.js';

/** How a node takes a write from below: it orders it as the owner, or passes it up. `via` is the node it came from */
export type MeshWriter = (address: string, change: Change, access?: Access, via?: string) => Promise<WriteOutcome>;

/** What serves a tunnelled session: an authority, or anything that takes connections as one does */
export type SessionEndpoint = Pick<Authority, 'connect'>;

export interface MeshServerOptions {
    readonly nodeId: string;
    readonly role: NodeRole;
    readonly keys: KeyPair;
    /** This node's certificate, or what gives its newest one (it is renewed while the server runs) */
    readonly certificate: string | (() => string);
    /** Magnetic's certificate authority: a node below must show a certificate it signed */
    readonly authorityPublicKey: string;
    /**
     * The roles whose nodes must be in `directory()` with the role and the key their certificate names
     * (docs/design/statenet-network.md): a certificate alone does not open a link for them, and a node taken out of
     * the directory loses its link at the next `directoryChanged`. None by default
     */
    readonly requireListed?: readonly NodeRole[];
    /**
     * Saves a namespace a node below sent up, and loads one: an aggregate's own, or a relay's link up to one. `node`
     * is the node that orders it. Without them this node keeps nothing for others
     */
    readonly persist?: (address: string, stored: StoredNamespace, epoch: number, node: string) => Promise<PersistOutcome>;
    readonly load?: (address: string, node: string) => Promise<LoadOutcome>;
    /** Reads the directory again, for a node that says hello and is not listed: it may have registered a moment ago */
    readonly reread?: () => Promise<void>;
    /**
     * Answers a node's ask for a certificate's next one: `certificate` is one the authority signed and has not ended,
     * `claims` what it says, `via` the node on the link (the node itself, or a relay that passes the ask up).
     * Without it nothing is renewed here
     */
    readonly certify?: (certificate: string, claims: NodeCertificateClaims, via: NodeCertificateClaims) => Promise<string | undefined>;
    /** What this node serves to the nodes below: an owner's authorities, or its own relay */
    readonly upstream: Upstream;
    readonly write: MeshWriter;
    /** The directory the nodes below learn; read when a link opens */
    readonly directory: () => DirectoryView;
    /**
     * The sessions of a namespace this node owns, for a peer that serves a view of it (an owner node of the same
     * statenet): the endpoint, or why not (it is owned elsewhere now). Without it a peer cannot open sessions here
     */
    readonly sessions?: (address: string, access: Access | undefined, peer: string) => Promise<SessionEndpoint | string>;
    /** The owner's side of a claim by the peer at the other end (`OwnerNode.handoff`) */
    readonly handoff?: (address: string, to: string) => Promise<Handoff | undefined>;
    /** The owner at the other end asks this node to claim a namespace (`OwnerNode.claim`) */
    readonly take?: (address: string) => Promise<boolean>;
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
    /** What seals every message of the link after its handshake */
    readonly seal: LinkSeal;
    readonly served: Map<number, Served>;
    /** The sessions tunnelled for the node below, by its tunnel id */
    readonly tunnels: Map<number, AuthoritySession>;
    /** Tunnels still being opened: what arrives for them meanwhile, and whether they were shut */
    readonly opening: Map<number, { said: string[]; shut: boolean }>;
    /** The writes of handoffs to the node below, waiting for their outcome, by `<cid>/<qid>` */
    readonly handed: Map<string, (outcome: WriteOutcome) => void>;
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
 * the node's key each way, with the shares of the key that seals everything after (`sealOf`). It refuses a role that
 * does not link to its own (`mayLink`), and with `requireListed` a node the directory does not list. It then serves the node's subscriptions from `upstream` and passes its writes to `write`, in
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

    /** A message of the handshake, in the clear */
    const send = (socket: WebSocket, message: LinkMessage): void => {
        if (socket.readyState === socket.OPEN) socket.send(JSON.stringify(message));
    };
    /** A message of an open link, sealed */
    const sendSealed = (link: Link, message: LinkMessage): void => {
        if (link.socket.readyState === link.socket.OPEN) link.socket.send(link.seal.seal(JSON.stringify(message)));
    };
    const certificate = (): string => (typeof options.certificate === 'string' ? options.certificate : options.certificate());
    const listed = (claims: NodeCertificateClaims): boolean =>
        options.requireListed?.includes(claims.role) !== true || options.directory().nodes.some((node) => node.id === claims.nodeId && node.role === claims.role && node.publicKey === claims.publicKey);

    const flush = (link: Link): void => {
        link.scheduled = false;
        if (link.outbox.length === 0) return;
        sendSealed(link, { type: 'frame', messages: link.outbox.splice(0) });
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
            case 'open': {
                const tid = message.tid;
                const pending = { said: [] as string[], shut: false };
                link.opening.set(tid, pending);
                const endpoint = options.sessions === undefined ? 'this node serves no sessions' : await options.sessions(message.address, message.access, link.peer.nodeId).catch((error: unknown) => (error instanceof Error ? error.message : String(error)));
                link.opening.delete(tid);
                if (typeof endpoint === 'string') {
                    if (!pending.shut) queue(link, { type: 'shut', tid, reason: endpoint });
                    return;
                }
                if (!links.has(link) || pending.shut) return;
                let open = true;
                const session = endpoint.connect(
                    {
                        send: (text) => {
                            if (open) queue(link, { type: 'say', tid, text });
                        },
                        close: () => {
                            if (!open) return;
                            open = false;
                            link.tunnels.delete(tid);
                            queue(link, { type: 'shut', tid, reason: 'closed' });
                        }
                    },
                    message.access
                );
                if (open) link.tunnels.set(tid, session);
                for (const text of pending.said) if (open) session.receive(text);
                return;
            }
            case 'say': {
                const pending = link.opening.get(message.tid);
                if (pending !== undefined) pending.said.push(message.text);
                else link.tunnels.get(message.tid)?.receive(message.text);
                return;
            }
            case 'shut': {
                const pending = link.opening.get(message.tid);
                if (pending !== undefined) pending.shut = true;
                const session = link.tunnels.get(message.tid);
                link.tunnels.delete(message.tid);
                session?.close();
                return;
            }
            case 'claim': {
                const handoff = await options.handoff?.(message.address, link.peer.nodeId).catch(() => undefined);
                if (handoff === undefined) {
                    queue(link, { type: 'handoff', cid: message.cid });
                    return;
                }
                const queued = handoff.queued.map((item, qid) => {
                    link.handed.set(`${message.cid}/${qid}`, item.done);
                    return { qid, change: item.change };
                });
                queue(link, { type: 'handoff', cid: message.cid, handoff: { revision: handoff.revision, epoch: handoff.epoch, queued } });
                return;
            }
            case 'handed': {
                const key = `${message.cid}/${message.qid}`;
                const done = link.handed.get(key);
                link.handed.delete(key);
                done?.(message.outcome);
                return;
            }
            case 'take': {
                const ok = (await options.take?.(message.address).catch(() => false)) ?? false;
                queue(link, { type: 'taken', cid: message.cid, ok });
                return;
            }
            case 'persist': {
                // a relay names the node it passes the save on for; anything else speaks for itself
                const node = link.peer.role === 'relay' && typeof message.node === 'string' ? message.node : link.peer.nodeId;
                const failed = (reason: string): PersistOutcome => ({ result: 'failed', reason });
                const outcome = options.persist === undefined ? failed(`${options.nodeId} keeps nothing for other nodes`) : await options.persist(message.address, message.stored, message.epoch, node).catch((error: unknown) => failed(error instanceof Error ? error.message : String(error)));
                queue(link, { type: 'persisted', pid: message.pid, outcome });
                return;
            }
            case 'load': {
                const node = link.peer.role === 'relay' && typeof message.node === 'string' ? message.node : link.peer.nodeId;
                const failed = (reason: string): LoadOutcome => ({ ok: false, reason });
                const outcome = options.load === undefined ? failed(`${options.nodeId} keeps nothing for other nodes`) : await options.load(message.address, node).catch((error: unknown) => failed(error instanceof Error ? error.message : String(error)));
                queue(link, { type: 'loaded', lid: message.lid, outcome });
                return;
            }
            case 'certify': {
                const checked = verifyNodeCertificate(message.certificate, options.authorityPublicKey, now());
                // a node asks for itself; only a relay passes on the ask of a node below it
                const allowed = checked.ok && (checked.claims.nodeId === link.peer.nodeId || link.peer.role === 'relay');
                const next = allowed ? await options.certify?.(message.certificate, checked.claims, link.peer).catch(() => undefined) : undefined;
                queue(link, { type: 'certified', rid: message.rid, ...(next === undefined ? {} : { certificate: next }) });
                return;
            }
            case 'write': {
                const outcome = await options.write(message.address, message.change, message.access, link.peer.nodeId).catch((error: unknown): WriteOutcome => ({ result: 'rejected', reason: error instanceof Error ? error.message : String(error) }));
                queue(link, { type: 'written', wid: message.wid, outcome });
                return;
            }
            default:
                return;
        }
    };

    wss.on('connection', (socket) => {
        const nonce = newNonce();
        const mine = newShare();
        let peer: NodeCertificateClaims | undefined;
        let greeted = false;
        let seal: LinkSeal | undefined;
        let facts: Parameters<typeof transcript>[2] | undefined;
        let link: Link | undefined;
        const refuse = (reason: string): void => {
            send(socket, { type: 'link-refused', reason });
            socket.close(4401, reason.slice(0, 100));
        };
        const timeout = setTimeout(() => {
            if (link === undefined) refuse('the link was not opened in time');
        }, 10_000);
        socket.on('message', (data, isBinary) => {
            if (link !== undefined) {
                // an open link carries sealed messages only: one that does not open was changed, replayed or lost one before it
                const text = isBinary ? link.seal.open(data) : undefined;
                const sealed = text === undefined ? undefined : parseLinkMessage(text);
                if (sealed === undefined) return socket.close(4400, 'a message on the link did not open');
                if (sealed.type === 'frame') for (const each of sealed.messages) void receive(link, each);
                else if (sealed.type === 'ping') sendSealed(link, { type: 'pong' });
                return;
            }
            const message = isBinary ? undefined : parseLinkMessage(data);
            if (message === undefined) return;
            if (message.type === 'link-hello' && peer === undefined && !greeted) {
                greeted = true;
                if (message.protocol !== MESH_PROTOCOL) return refuse(`this node speaks mesh protocol ${MESH_PROTOCOL}`);
                const checked = verifyNodeCertificate(message.certificate, options.authorityPublicKey, now());
                if (!checked.ok) return refuse(checked.reason);
                const claims = checked.claims;
                if (claims.nodeId !== message.nodeId) return refuse('the certificate is for another node');
                if (claims.role !== message.role) return refuse('the certificate is for another role');
                if (!mayLink(claims.role, options.role)) return refuse(`a node of role ${claims.role} does not link to one of role ${options.role}`);
                if (typeof message.nonce !== 'string' || typeof message.share !== 'string') return refuse('the hello has no challenge or no share');
                const sealed = sealOf(mine, message.share, { dialler: message.nonce, server: nonce }, 'server');
                if (sealed === undefined) return refuse('the share is not a key');
                const welcome = (): void => {
                    peer = claims;
                    seal = sealed;
                    facts = { diallerShare: message.share, serverShare: mine.share, dialler: claims.nodeId, server: options.nodeId };
                    send(socket, { type: 'link-welcome', nodeId: options.nodeId, certificate: certificate(), nonce, proof: proveNode(transcript('server', message.nonce, facts), options.keys.privateKey), share: mine.share });
                };
                if (listed(claims)) return welcome();
                if (options.reread === undefined) return refuse('the node is not in the directory');
                // a node that started a moment ago registered after the directory was last read: read it once more
                void options
                    .reread()
                    .catch(() => undefined)
                    .then(() => (listed(claims) ? welcome() : refuse('the node is not in the directory')));
                return;
            }
            if (message.type === 'link-proof' && peer !== undefined && seal !== undefined && facts !== undefined) {
                if (!checkProof(transcript('dialler', nonce, facts), message.proof, peer.publicKey)) return refuse('the node did not prove it holds its key');
                clearTimeout(timeout);
                link = { socket, peer, seal, served: new Map(), tunnels: new Map(), opening: new Map(), handed: new Map(), credit: 0, outbox: [], scheduled: false };
                links.add(link);
                // the first sealed frame starts with the directory: the node below knows by it that it was let in
                queue(link, { type: 'directory', directory: options.directory() });
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
            for (const session of link.tunnels.values()) session.close();
            link.tunnels.clear();
            for (const done of link.handed.values()) done({ result: 'rejected', reason: 'the link to the new owner closed' });
            link.handed.clear();
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
            for (const link of [...links]) {
                if (listed(link.peer)) queue(link, { type: 'directory', directory });
                else link.socket.close(4403, 'the node is no longer in the directory');
            }
        },
        close: async () => {
            for (const link of links) link.socket.terminate();
            await new Promise<void>((resolve) => wss.close(() => resolve()));
            await new Promise<void>((resolve) => http.close(() => resolve()));
        }
    };
}
