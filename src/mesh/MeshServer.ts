import { report } from '@punyts/core';
import { createServer as createHttpServer } from 'node:http';
import type { Server as HttpServer } from 'node:http';
import { createServer as createHttpsServer } from 'node:https';
import type { AddressInfo } from 'node:net';
import type { Access, Handoff, StoredNamespace, Upstream } from '@punyts/state-sync/authority';
import { WebSocketServer } from 'ws';
import type { WebSocket } from 'ws';
import { checkProof, mayLink, newNonce, proveNode, verifyNodeCertificate } from './certificates.js';
import type { KeyPair, NodeCertificateClaims, NodeRole } from './certificates.js';
import { createLinkClient } from './linkClient.js';
import type { LinkClient } from './linkClient.js';
import { MESH_PROTOCOL, parseLinkMessage } from './protocol.js';
import type { DirectoryView, LinkMessage, LoadOutcome, MeshMessage, PersistOutcome, RemoveOutcome } from './protocol.js';
import { newShare, sealOf, transcript } from './sealing.js';
import type { LinkSeal } from './sealing.js';
import { createServing } from './serving.js';
import type { MeshWriter, Serving, ServingOptions, SessionEndpoint } from './serving.js';

export type { MeshWriter, SessionEndpoint } from './serving.js';

/** The category of a mesh server's reports: node ids, roles and counts, never values */
const CATEGORY = 'statenet.mesh';

export interface MeshServerOptions {
    readonly nodeId: string;
    readonly role: NodeRole;
    /** The group of a node (`owner`): only nodes of the same group link to it directly */
    readonly group?: string;
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
    /** Removes a namespace a node below asks to have removed: an aggregate's own, or a relay's link up to one */
    readonly remove?: (address: string, epoch: number, node: string) => Promise<RemoveOutcome>;
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
    /** The way to a node an ask names (`ServingOptions.route`): a relay's */
    readonly route?: ServingOptions['route'];
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

interface Link {
    readonly socket: WebSocket;
    readonly peer: NodeCertificateClaims;
    /** What seals every message of the link after its handshake */
    readonly seal: LinkSeal;
    /** What this node does with the asks of the node below */
    readonly serving: Serving;
    /** What this node asks of the node below: the other way down the same link (`rev` messages) */
    readonly client: LinkClient;
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
    /**
     * The node linked below with this id, to ask it for what it owns (docs/design/statenet-network.md, S9: a relay
     * reaches the nodes below it); undefined while it has no link here. What was asked ends with the link
     */
    peer(nodeId: string): LinkClient | undefined;
    /** Sends a change of the directory down every link */
    directoryChanged(): void;
    close(): Promise<void>;
}

/**
 * The parent's side of the mesh protocol (docs/design/statenet-relays.md, "The mesh protocol"): one WebSocket per node
 * below, opened with `link-hello` and its certificate, checked against Magnetic's certificate authority, and a proof of
 * the node's key each way, with the shares of the key that seals everything after (`sealOf`). It refuses a role that
 * does not link to its own (`mayLink`), with `requireListed` a node the directory does not list, and a node beside it
 * of another group. It then serves the node's asks (`createServing`) in frames that batch what is ready at one moment,
 * and can ask the node in turn (`peer`)
 */
export async function startMeshServer(options: MeshServerOptions): Promise<MeshServer> {
    const now = options.now ?? Date.now;
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

    wss.on('connection', (socket) => {
        const nonce = newNonce();
        const mine = newShare();
        let peer: NodeCertificateClaims | undefined;
        let greeted = false;
        let seal: LinkSeal | undefined;
        let facts: Parameters<typeof transcript>[2] | undefined;
        let link: Link | undefined;
        const refuse = (reason: string): void => {
            report.warning('Node %s refused a link from %s: %s', [options.nodeId, peer?.nodeId ?? 'a node', reason]);
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
                if (sealed.type === 'frame') {
                    // `rev` answers what this node asked of the node below; the rest are the node's own asks
                    for (const each of sealed.messages) {
                        if (each.type === 'rev') link.client.receive(each.message);
                        else void link.serving.receive(each);
                    }
                } else if (sealed.type === 'ping') sendSealed(link, { type: 'pong' });
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
                // nodes link to each other only inside a group: between groups they go through their relays
                if (claims.role === 'owner' && options.role === 'owner' && (claims.group ?? '') !== (options.group ?? '')) return refuse(`a node of group ${claims.group ?? '(none)'} does not link to a node of group ${options.group ?? '(none)'}`);
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
                // the first sealed frame starts with the directory: the node below knows by it that it was let in
                const outbox: MeshMessage[] = [{ type: 'directory', directory: options.directory() }];
                const ask = (message: MeshMessage): void => {
                    if (link !== undefined) queue(link, message);
                    else outbox.push(message);
                };
                const opened: Link = {
                    socket,
                    peer,
                    seal,
                    serving: createServing(options, peer, ask, () => link !== undefined && links.has(link)),
                    client: createLinkClient((message) => ask({ type: 'rev', message })),
                    outbox,
                    scheduled: false
                };
                link = opened;
                links.add(link);
                report(CATEGORY, 'Node %s opened a link from %s (%s): %s links', [options.nodeId, opened.peer.nodeId, opened.peer.role, links.size]);
                opened.scheduled = true;
                queueMicrotask(() => flush(opened));
                return;
            }
            refuse('open the link with link-hello and link-proof first');
        });
        socket.on('close', () => {
            clearTimeout(timeout);
            if (link === undefined) return;
            links.delete(link);
            report(CATEGORY, 'The link from %s to node %s closed: %s links', [link.peer.nodeId, options.nodeId, links.size]);
            link.serving.close();
            link.client.close();
        });
        socket.on('error', () => undefined);
    });

    await new Promise<void>((resolve) => http.listen(options.port ?? 0, options.host ?? '127.0.0.1', resolve));
    const address = http.address() as AddressInfo;
    const host = address.family === 'IPv6' ? `[${address.address}]` : address.address;
    report(CATEGORY, 'Node %s (%s) takes links at %s', [options.nodeId, options.role, `${options.tls === undefined ? 'ws' : 'wss'}://${host}:${address.port}`]);
    return {
        url: `${options.tls === undefined ? 'ws' : 'wss'}://${host}:${address.port}`,
        stats: () => ({
            links: [...links].map((link) => ({ nodeId: link.peer.nodeId, ...link.serving.stats() }))
        }),
        peer: (nodeId) => [...links].reverse().find((link) => link.peer.nodeId === nodeId)?.client,
        directoryChanged: () => {
            const directory = options.directory();
            for (const link of [...links]) {
                if (listed(link.peer)) queue(link, { type: 'directory', directory });
                else {
                    report(CATEGORY, 'Node %s closes the link from %s: it is no longer in the directory', [options.nodeId, link.peer.nodeId]);
                    link.socket.close(4403, 'the node is no longer in the directory');
                }
            }
        },
        close: async () => {
            for (const link of links) link.socket.terminate();
            await new Promise<void>((resolve) => wss.close(() => resolve()));
            await new Promise<void>((resolve) => http.close(() => resolve()));
        }
    };
}
