import { isReported, report } from '@punyts/core';
import { createServer } from 'node:http';
import type { IncomingMessage } from 'node:http';
import type { AddressInfo } from 'node:net';
import { createSyncWire } from '@punyts/state-sync';
import type { SyncMessage, SyncRequest, SyncWire, WelcomeMessage } from '@punyts/state-sync';
import type { Access, AuthorityConnection, AuthoritySession, RelaySubscription, Upstream } from '@punyts/state-sync/authority';
import { WebSocketServer } from 'ws';
import type { MeshWriter } from './MeshServer.js';

/** The category of a relay's browser side: addresses, revisions, paths and outcomes, never values */
const CATEGORY = 'statenet.relay.client';

/** A namespace name of the sync protocol for an address: `<owner>.<app>.<scope>` for `<owner>/<app>/<scope>` */
export const namespaceOfAddress = (address: string): string => address.split('/').join('.');
/** The address a namespace name stands for, or undefined */
export const addressOfNamespace = (namespace: string): string | undefined => {
    const parts = namespace.split('.');
    return parts.length === 3 ? parts.join('/') : undefined;
};

export interface ClientServerOptions {
    /** What this node serves: its relay */
    readonly upstream: Upstream;
    /** Where the clients' changes go: up the mesh to the owner node */
    readonly write: MeshWriter;
    /**
     * A session with the owner's authority, tunnelled up the mesh: a client's requests go through one that
     * follows requests only (docs/design/statenet-requests.md). Without it, a client gets no requests
     */
    readonly open?: (address: string, connection: AuthorityConnection, access: Access | undefined) => AuthoritySession;
    /** How long a welcome waits for the client's requests before it goes without them; 5 seconds by default */
    readonly requestsWaitMs?: number;
    /** Who a client is and the grant it presents, from its request (a session cookie, a token) */
    readonly accessOf?: (request: IncomingMessage) => Access | undefined;
    readonly host?: string;
    readonly port?: number;
    /** How often a client's subscription lease is renewed while its socket is open; 10 seconds by default */
    readonly renewEveryMs?: number;
}

export interface ClientServer {
    readonly url: string;
    close(): Promise<void>;
}

/**
 * The browsers' side of a relay (docs/design/statenet-relays.md, "The mesh protocol": "A browser or an app's session
 * connects to a relay and never sees the mesh"): state-sync's own protocol, version 4, for the namespace of an address.
 * A `hello` subscribes the relay to the whole namespace from the client's revision; the relay's welcome, changes and
 * watermark go to the client as `welcome`, `changes` and `durable`; a client's `change` goes up the mesh to the owner
 * node and its outcome comes back as the `ack`. A gap makes the client say hello again, and it is caught up the same way.
 * A client's requests (docs/design/statenet-requests.md) come from a session with the owner's authority that
 * follows requests only: its welcome's requests go into the client's welcome, which waits for them, its `requested` and
 * `decided` go to the client, and the client's `approve`, `reject` and `withdraw` go to it
 */
export async function startClientServer(options: ClientServerOptions): Promise<ClientServer> {
    const http = createServer();
    const wss = new WebSocketServer({ server: http, maxPayload: 4 * 1024 * 1024 });
    wss.on('connection', (socket, request) => {
        const access = options.accessOf?.(request);
        /** The browser's wire, for as long as its socket is open, and the tunnel's, for as long as the tunnel */
        const wire = createSyncWire();
        let tunnelWire: SyncWire | undefined;
        let subscription: RelaySubscription | undefined;
        let renew: ReturnType<typeof setInterval> | undefined;
        let address: string | undefined;
        /** The tunnel of the client's requests, the requests known from it (undefined until its welcome), and what waits for them */
        let tunnel: AuthoritySession | undefined;
        let known: Map<string, SyncRequest> | undefined;
        let held: WelcomeMessage | undefined;
        let early: SyncMessage[] = [];
        let wait: ReturnType<typeof setTimeout> | undefined;
        const send = (message: SyncMessage): void => {
            if (socket.readyState === socket.OPEN) socket.send(wire.write(message));
        };
        /** Sends the held welcome, with the requests when they are known, and what came after it */
        const release = (): void => {
            if (wait !== undefined) clearTimeout(wait);
            wait = undefined;
            if (held === undefined) return;
            send(known === undefined ? held : { ...held, requests: [...known.values()] });
            held = undefined;
            for (const message of early.splice(0)) send(message);
        };
        /** A message for the client: held behind a welcome that waits */
        const pass = (message: SyncMessage): void => {
            if (held === undefined) send(message);
            else early.push(message);
        };
        const fromTunnel = (data: string | Uint8Array): void => {
            const parsed = tunnelWire?.read(data);
            if (parsed === undefined || !parsed.ok || known === undefined && parsed.message.type !== 'welcome') return;
            const message = parsed.message;
            if (message.type === 'welcome') {
                known = new Map((message.requests ?? []).map((request) => [request.id, request]));
                release();
            } else if (message.type === 'requested') {
                known?.set(message.request.id, message.request);
                pass(message);
            } else if (message.type === 'decided') {
                const request = known?.get(message.id);
                if (request !== undefined) {
                    known?.set(message.id, { ...request, status: message.status, decidedAt: message.decidedAt, ...(message.code === undefined ? {} : { code: message.code }), ...(message.note === undefined ? {} : { note: message.note }) });
                }
                pass(message);
            }
        };
        const stop = (): void => {
            subscription?.close();
            subscription = undefined;
            if (renew !== undefined) clearInterval(renew);
            renew = undefined;
            tunnel?.close();
            tunnel = undefined;
            known = undefined;
            held = undefined;
            early = [];
            if (wait !== undefined) clearTimeout(wait);
            wait = undefined;
        };
        socket.on('message', (data, isBinary) => {
            const buffer = Array.isArray(data) ? Buffer.concat(data) : Buffer.isBuffer(data) ? data : Buffer.from(data);
            const parsed = wire.read(isBinary ? new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength).slice() : buffer.toString('utf8'));
            if (!parsed.ok) {
                report(CATEGORY, 'Refused a message from a browser: %s', [parsed.error.code]);
                send({ type: 'error', code: parsed.error.code, message: parsed.error.message });
                return;
            }
            const message = parsed.message;
            if (message.type === 'hello') {
                stop();
                address = addressOfNamespace(message.namespace);
                if (address === undefined) {
                    report(CATEGORY, 'Refused a hello for %s: not <owner>.<app>.<scope>', [message.namespace]);
                    send({ type: 'error', code: 'wrong-namespace', message: 'a namespace here is <owner>.<app>.<scope>' });
                    return;
                }
                const namespace = message.namespace;
                report(CATEGORY, 'A browser subscribes to %s from revision %s', [address, message.revision]);
                if (options.open !== undefined) {
                    const connection: AuthorityConnection = {
                        send: fromTunnel,
                        close: () => {
                            if (tunnel === undefined) return;
                            tunnel = undefined;
                            report(CATEGORY, 'The requests of a browser on %s closed', [namespace]);
                            release();
                        }
                    };
                    const opened = createSyncWire();
                    tunnelWire = opened;
                    tunnel = options.open(address, connection, access);
                    tunnel.receive(opened.write({ type: 'hello', namespace, revision: 0, clientId: message.clientId, protocol: 4, requests: true }));
                }
                const welcomed = (welcome: WelcomeMessage): void => {
                    if (tunnel === undefined || known !== undefined) {
                        send(known === undefined ? welcome : { ...welcome, requests: [...known.values()] });
                        return;
                    }
                    held = welcome;
                    wait ??= setTimeout(release, options.requestsWaitMs ?? 5_000);
                };
                subscription = options.upstream.subscribe(
                    address,
                    '$',
                    message.revision > 0 ? message.revision : undefined,
                    {
                        welcome: (welcome) =>
                            welcomed(
                                'value' in welcome
                                    ? { type: 'welcome', namespace, revision: welcome.revision, snapshot: (welcome.value ?? {}) as never, ...(welcome.durable === undefined ? {} : { durable: welcome.durable }) }
                                    : { type: 'welcome', namespace, revision: welcome.revision, changes: [...welcome.changes], ...(welcome.durable === undefined ? {} : { durable: welcome.durable }) }
                            ),
                        changes: (batch) => pass({ type: 'changes', changes: [...batch.changes] }),
                        durable: (revision) => pass({ type: 'durable', revision }),
                        closed: (reason) => {
                            report(CATEGORY, 'The subscription of a browser to %s closed: %s', [namespace, reason]);
                            send({ type: 'error', code: reason === 'revoked' ? 'revoked' : 'closed', message: reason });
                            socket.close();
                        }
                    },
                    access
                );
                renew = setInterval(() => subscription?.renew(), options.renewEveryMs ?? 10_000);
                (renew as { unref?: () => void }).unref?.();
                return;
            }
            if ((message.type === 'approve' || message.type === 'reject' || message.type === 'withdraw') && tunnel !== undefined) {
                tunnel.receive((tunnelWire as SyncWire).write(message));
                return;
            }
            if (message.type === 'change' && address !== undefined) {
                const change = message.change;
                const written = address;
                void options.write(address, change, access).then((outcome) => {
                    if (isReported(CATEGORY)) report(CATEGORY, 'A browser\'s change of %s at %s: %s', [written, change.path, outcome.result]);
                    switch (outcome.result) {
                        case 'rejected':
                            pass({ type: 'ack', id: change.id, result: 'rejected', reason: outcome.reason, ...(outcome.code === undefined ? {} : { code: outcome.code }) });
                            return;
                        case 'requested':
                        case 'passed':
                            pass({ type: 'ack', id: change.id, result: outcome.result });
                            return;
                        case 'adjusted':
                            pass({ type: 'ack', id: change.id, result: 'adjusted', revision: outcome.revision, value: outcome.value ?? null });
                            return;
                        default:
                            pass({ type: 'ack', id: change.id, result: 'applied', revision: outcome.revision });
                    }
                });
            }
        });
        socket.on('close', stop);
        socket.on('error', () => undefined);
    });
    await new Promise<void>((resolve) => http.listen(options.port ?? 0, options.host ?? '127.0.0.1', resolve));
    const { port } = http.address() as AddressInfo;
    return {
        url: `ws://${options.host ?? '127.0.0.1'}:${port}`,
        close: async () => {
            for (const client of wss.clients) client.terminate();
            await new Promise<void>((resolve) => wss.close(() => resolve()));
            await new Promise<void>((resolve) => http.close(() => resolve()));
        }
    };
}
