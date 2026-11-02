import { createServer } from "node:http";
import type { IncomingMessage, Server, ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import type { Duplex } from "node:stream";
import { report } from "@punyts/core";
import { parseMessage } from "@punyts/state-sync";
import type { ErrorMessage } from "@punyts/state-sync";
import { createAuthority } from "@punyts/state-sync/authority";
import type { Authority, AuthoritySession, NamespaceStorage } from "@punyts/state-sync/authority";
import { WebSocketServer } from "ws";
import type { RawData, WebSocket } from "ws";
import { createFileNamespaceStorage } from "./FileNamespaceStorage.js";
import { hostNameOf, isLoopbackHost } from "./loopback.js";

/** The path of the WebSocket endpoint */
export const SYNC_PATH = "/sync";
export const DEFAULT_PORT = 4200;
export const DEFAULT_LOG_LENGTH = 1000;
/** The most messages a connection may send while its namespace loads */
const MAX_PENDING = 100;
const KEEP_ALIVE_MS = 30_000;
const MAX_FRAME_BYTES = 4 * 1024 * 1024;

export interface StatenetOptions {
    /** A loopback address; anything else is refused. `127.0.0.1` by default */
    host?: string;
    /** 4200 by default; 0 picks a free port */
    port?: number;
    /** The folder of the JSON file storage; needed unless `storage` is given */
    dataDir?: string;
    /** The storage port; the JSON file storage under `dataDir` by default */
    storage?: NamespaceStorage;
    /** The most recent changes each namespace keeps for catching clients up; 1000 by default */
    logLength?: number;
    /**
     * The browser origins (`http://127.0.0.1:4100`) that may connect. A connection that sends an `Origin` header is
     * refused unless its origin is listed; clients that send none (Node, app servers) are accepted. None by default
     */
    allowedOrigins?: readonly string[];
}

export interface RunningStatenet {
    /** The WebSocket URL clients connect to: `ws://<host>:<port>/sync` */
    url: string;
    port: number;
    /** The namespaces loaded so far */
    namespaces: () => string[];
    /** Closes every connection, waits for the pending saves and stops listening */
    close: () => Promise<void>;
}

const errorMessage = (code: string, message: string): string => JSON.stringify({ type: "error", code, message } satisfies ErrorMessage);

const textOf = (data: RawData): string =>
    Array.isArray(data) ? Buffer.concat(data).toString("utf8") : Buffer.from(data as ArrayBuffer).toString("utf8");

/**
 * Starts the statenet: one authority per namespace, with no server logic of its own, behind a WebSocket endpoint at
 * `/sync`. A namespace starts empty and creates missing parents, since the statenet does not know its shape. A
 * connection names its namespace with its first message, the protocol's `hello`; the namespace is loaded from
 * storage when it is first asked for. It listens on loopback only, since it has no authentication
 */
export const startStatenet = async (options: StatenetOptions): Promise<RunningStatenet> => {
    const host = options.host ?? "127.0.0.1";
    if (!isLoopbackHost(host)) throw new Error(`The statenet listens on loopback only, not on ${host}: it has no authentication`);
    if (options.storage === undefined && options.dataDir === undefined) throw new Error("The statenet needs a data folder or a storage");
    const storage = options.storage ?? createFileNamespaceStorage({ dataDir: options.dataDir as string });
    const logLength = options.logLength ?? DEFAULT_LOG_LENGTH;
    const allowedOrigins = new Set(options.allowedOrigins ?? []);
    const authorities = new Map<string, Promise<Authority>>();

    const authorityOf = (namespace: string): Promise<Authority> => {
        let authority = authorities.get(namespace);
        if (authority === undefined) {
            authority = createAuthority({ namespace, storage, logLength, createParents: true }).then((created) => {
                report("statenet", "Loaded namespace %s at revision %s", [namespace, created.revision()]);
                return created;
            });
            authority.catch(() => authorities.delete(namespace));
            authorities.set(namespace, authority);
        }
        return authority;
    };

    const sockets = new WebSocketServer({ noServer: true, maxPayload: MAX_FRAME_BYTES });

    const onConnection = (ws: WebSocket): void => {
        let session: AuthoritySession | undefined;
        let opening = false;
        let closed = false;
        const pending: string[] = [];
        const timer = setInterval(() => ws.ping(), KEEP_ALIVE_MS);
        const refuse = (code: string, message: string, closeCode: number): void => {
            if (ws.readyState === ws.OPEN) ws.send(errorMessage(code, message));
            ws.close(closeCode, code);
        };

        ws.on("message", (data, isBinary) => {
            if (isBinary) return refuse("binary", "Text frames only", 1003);
            const text = textOf(data);
            if (session !== undefined) return session.receive(text);
            if (opening) {
                if (pending.length >= MAX_PENDING) return refuse("too-many", "Too many messages before the namespace loaded", 1008);
                pending.push(text);
                return;
            }
            const parsed = parseMessage(text);
            if (!parsed.ok) return refuse(parsed.error.code, parsed.error.message, 1008);
            if (parsed.message.type !== "hello") return refuse("hello-first", "The first message must be hello", 1008);
            const { namespace } = parsed.message;
            opening = true;
            authorityOf(namespace)
                .then((authority) => {
                    if (closed) return;
                    session = authority.connect({
                        send: (message) => {
                            if (ws.readyState === ws.OPEN) ws.send(message);
                        },
                        close: () => ws.close()
                    });
                    report("statenet", "A client joined %s", [namespace]);
                    session.receive(text);
                    for (const message of pending.splice(0)) session.receive(message);
                })
                .catch((error: unknown) => {
                    report("statenet", "Could not load namespace %s: %s", [namespace, error instanceof Error ? error.message : String(error)]);
                    refuse("unavailable", "The namespace could not be loaded", 1011);
                });
        });
        ws.on("error", () => ws.terminate());
        ws.on("close", () => {
            closed = true;
            clearInterval(timer);
            session?.close();
        });
    };

    const refuseUpgrade = (socket: Duplex, status: number, reason: string): void => {
        socket.end(`HTTP/1.1 ${status} ${reason}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
    };

    const isAllowed = (request: IncomingMessage): boolean => {
        // A loopback Host keeps a page on another name, rebound to 127.0.0.1, out
        const hostName = hostNameOf(request.headers.host);
        if (hostName === undefined || !isLoopbackHost(hostName)) return false;
        const origin = request.headers.origin;
        return origin === undefined || allowedOrigins.has(origin);
    };

    const onRequest = (request: IncomingMessage, response: ServerResponse): void => {
        const path = (request.url ?? "/").split("?")[0];
        if (request.method === "GET" && path === "/health" && isAllowed(request)) {
            response.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
            response.end(JSON.stringify({ ok: true }));
            return;
        }
        response.writeHead(404, { "content-type": "text/plain" });
        response.end("Not found");
    };

    const server: Server = createServer(onRequest);
    server.on("upgrade", (request: IncomingMessage, socket: Duplex, head: Buffer) => {
        const path = (request.url ?? "/").split("?")[0];
        if (path !== SYNC_PATH) return refuseUpgrade(socket, 404, "Not Found");
        if (!isAllowed(request)) return refuseUpgrade(socket, 403, "Forbidden");
        sockets.handleUpgrade(request, socket, head, onConnection);
    });

    await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(options.port ?? DEFAULT_PORT, host, () => {
            server.off("error", reject);
            resolve();
        });
    });
    const address = server.address() as AddressInfo;
    const hostForUrl = address.family === "IPv6" ? `[${address.address}]` : address.address;
    report("statenet", "Listening on %s", [`ws://${hostForUrl}:${address.port}${SYNC_PATH}`]);

    return {
        url: `ws://${hostForUrl}:${address.port}${SYNC_PATH}`,
        port: address.port,
        namespaces: () => [...authorities.keys()],
        close: async () => {
            const loaded = await Promise.all([...authorities.values()].map((authority) => authority.catch(() => undefined)));
            for (const authority of loaded) authority?.close();
            for (const client of sockets.clients) client.terminate();
            sockets.close();
            await Promise.all(loaded.map((authority) => authority?.flush()));
            authorities.clear();
            await new Promise<void>((resolve) => server.close(() => resolve()));
        }
    };
};
