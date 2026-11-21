import { isReported, report } from '@punyts/core';
import { createServer } from 'node:http';
import type { IncomingMessage } from 'node:http';
import type { AddressInfo } from 'node:net';
import { parseMessage } from '@punyts/state-sync';
import type { SyncMessage } from '@punyts/state-sync';
import type { Access, RelaySubscription, Upstream } from '@punyts/state-sync/authority';
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
 * connects to a relay and never sees the mesh"): state-sync's own protocol, version 2, for the namespace of an address.
 * A `hello` subscribes the relay to the whole namespace from the client's revision; the relay's welcome, changes and
 * watermark go to the client as `welcome`, `changes` and `durable`; a client's `change` goes up the mesh to the owner
 * node and its outcome comes back as the `ack`. A gap makes the client say hello again, and it is caught up the same way
 */
export async function startClientServer(options: ClientServerOptions): Promise<ClientServer> {
    const http = createServer();
    const wss = new WebSocketServer({ server: http, maxPayload: 4 * 1024 * 1024 });
    wss.on('connection', (socket, request) => {
        const access = options.accessOf?.(request);
        let subscription: RelaySubscription | undefined;
        let renew: ReturnType<typeof setInterval> | undefined;
        let address: string | undefined;
        const send = (message: SyncMessage): void => {
            if (socket.readyState === socket.OPEN) socket.send(JSON.stringify(message));
        };
        const stop = (): void => {
            subscription?.close();
            subscription = undefined;
            if (renew !== undefined) clearInterval(renew);
            renew = undefined;
        };
        socket.on('message', (data) => {
            const parsed = parseMessage(data.toString());
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
                subscription = options.upstream.subscribe(
                    address,
                    '$',
                    message.revision > 0 ? message.revision : undefined,
                    {
                        welcome: (welcome) =>
                            send(
                                'value' in welcome
                                    ? { type: 'welcome', namespace, revision: welcome.revision, snapshot: (welcome.value ?? {}) as never, ...(welcome.durable === undefined ? {} : { durable: welcome.durable }) }
                                    : { type: 'welcome', namespace, revision: welcome.revision, changes: [...welcome.changes], ...(welcome.durable === undefined ? {} : { durable: welcome.durable }) }
                            ),
                        changes: (batch) => send({ type: 'changes', changes: [...batch.changes] }),
                        durable: (revision) => send({ type: 'durable', revision }),
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
            if (message.type === 'change' && address !== undefined) {
                const change = message.change;
                const written = address;
                void options.write(address, change, access).then((outcome) => {
                    if (isReported(CATEGORY)) report(CATEGORY, 'A browser\'s change of %s at %s: %s', [written, change.path, outcome.result]);
                    send(
                        outcome.result === 'rejected'
                            ? { type: 'ack', id: change.id, result: 'rejected', reason: outcome.reason }
                            : outcome.result === 'adjusted'
                              ? { type: 'ack', id: change.id, result: 'adjusted', revision: outcome.revision, value: outcome.value ?? null }
                              : { type: 'ack', id: change.id, result: 'applied', revision: outcome.revision }
                    );
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
