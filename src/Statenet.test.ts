import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { addListener, removeListener, setCategories } from "@punyts/core";
import { createSyncWire } from "@punyts/state-sync";
import type { SyncMessage } from "@punyts/state-sync";
import { WebSocket } from "ws";
import { NAMESPACE_FILE } from "./FileNamespaceStorage.js";
import { LEFT_EMIT, startStatenet } from "./Statenet.js";
import type { RunningStatenet } from "./Statenet.js";

type Message = Record<string, unknown>;

interface TestClient {
    socket: WebSocket;
    received: Message[];
    next: (match: (message: Message) => boolean) => Promise<Message>;
    send: (message: object) => void;
    closed: Promise<number>;
}

const connect = (url: string, headers: Record<string, string> = {}): Promise<TestClient> => {
    const socket = new WebSocket(url, { headers });
    const received: Message[] = [];
    const waits: { match: (message: Message) => boolean; resolve: (message: Message) => void }[] = [];
    const wire = createSyncWire();
    socket.on("message", (data, isBinary) => {
        const buffer = Array.isArray(data) ? Buffer.concat(data) : Buffer.isBuffer(data) ? data : Buffer.from(data);
        const parsed = wire.read(isBinary ? new Uint8Array(buffer) : buffer.toString("utf8"));
        if (!parsed.ok) throw new Error(`the statenet sent what the wire cannot read: ${parsed.error.message}`);
        const message = parsed.message as unknown as Message;
        received.push(message);
        for (const wait of [...waits]) {
            if (!wait.match(message)) continue;
            waits.splice(waits.indexOf(wait), 1);
            wait.resolve(message);
        }
    });
    const closed = new Promise<number>((resolve) => socket.once("close", (code) => resolve(code)));
    const next = (match: (message: Message) => boolean): Promise<Message> => {
        const found = received.find(match);
        if (found !== undefined) return Promise.resolve(found);
        return new Promise((resolve, reject) => {
            waits.push({ match, resolve });
            setTimeout(() => reject(new Error("no matching message")), 5000);
        });
    };
    return new Promise((resolve, reject) => {
        socket.once("open", () => resolve({ socket, received, next, send: (message) => socket.send(wire.write(message as SyncMessage)), closed }));
        socket.once("error", reject);
    });
};

const hello = (client: TestClient, namespace: string, revision: number, clientId: string): Promise<Message> => {
    client.send({ type: "hello", namespace, revision, clientId, protocol: 4 });
    return client.next((message) => message["type"] === "welcome");
};

const upgradeStatus = (url: string, headers: Record<string, string>): Promise<number> =>
    new Promise((resolve, reject) => {
        const socket = new WebSocket(url, { headers });
        socket.once("unexpected-response", (_request, response) => {
            resolve(response.statusCode ?? 0);
            socket.terminate();
        });
        socket.once("open", () => {
            socket.close();
            reject(new Error("the upgrade was accepted"));
        });
        socket.once("error", () => undefined);
    });

describe("startStatenet", () => {
    let dataDir = "";
    let statenet: RunningStatenet | undefined;
    const clients: TestClient[] = [];

    const open = async (headers?: Record<string, string>): Promise<TestClient> => {
        const client = await connect((statenet as RunningStatenet).url, headers);
        clients.push(client);
        return client;
    };

    beforeEach(async () => {
        dataDir = await mkdtemp(join(tmpdir(), "statenet-test-"));
        statenet = await startStatenet({ dataDir, port: 0, allowedOrigins: ["http://127.0.0.1:4100"] });
    });

    afterEach(async () => {
        for (const client of clients.splice(0)) client.socket.terminate();
        await statenet?.close();
        statenet = undefined;
        await rm(dataDir, { recursive: true, force: true });
    });

    it("should sync a change from one client to another in the same namespace", async () => {
        // Arrange
        const first = await open();
        const second = await open();
        await hello(first, "apps.orders", 0, "first");
        await hello(second, "apps.orders", 0, "second");

        // Act
        first.send({ type: "change", change: { id: "c1", path: "$.data.total", op: "set", value: 42, base: 0 } });
        const ack = await first.next((message) => message["type"] === "ack");
        const pushed = await second.next((message) => message["type"] === "changes");

        // Assert
        expect(ack).toMatchObject({ id: "c1", result: "applied", revision: 1 });
        expect(pushed["changes"]).toEqual([expect.objectContaining({ id: "c1", path: "$.data.total", op: "set", value: 42, revision: 1 })]);
    });

    it("should keep namespaces apart", async () => {
        const orders = await open();
        const other = await open();
        await hello(orders, "apps.orders", 0, "orders");
        await hello(other, "apps.other", 0, "other");

        orders.send({ type: "change", change: { id: "c1", path: "$.data.total", op: "set", value: 1, base: 0 } });
        await orders.next((message) => message["type"] === "ack");
        await new Promise((resolve) => setTimeout(resolve, 100));

        expect(other.received.filter((message) => message["type"] === "changes")).toEqual([]);
        expect(statenet?.namespaces().sort()).toEqual(["apps.orders", "apps.other"]);
    });

    it("should keep the state and the revision across a restart", async () => {
        // Arrange
        const client = await open();
        await hello(client, "apps.orders", 0, "first");
        client.send({ type: "change", change: { id: "c1", path: "$.data.total", op: "set", value: 7, base: 0 } });
        await client.next((message) => message["type"] === "ack");
        client.send({ type: "change", change: { id: "c2", path: "$.data.note", op: "set", value: "hi", base: 1 } });
        await client.next((message) => message["type"] === "ack" && message["id"] === "c2");

        // Act
        await statenet?.close();
        statenet = await startStatenet({ dataDir, port: 0 });
        const fresh = await open();
        const welcome = await hello(fresh, "apps.orders", 0, "after-restart");
        const stored = JSON.parse(await readFile(join(dataDir, "apps.orders", NAMESPACE_FILE), "utf8")) as { revision: number };

        // Assert
        expect(welcome["revision"]).toBe(2);
        expect(stored.revision).toBe(2);
        const caughtUp = (welcome["changes"] as Message[] | undefined) ?? [];
        const snapshot = welcome["snapshot"] as { data: Message } | undefined;
        expect(snapshot?.data ?? Object.fromEntries(caughtUp.map((change) => [String(change["path"]).slice(7), change["value"]]))).toEqual({ total: 7, note: "hi" });
    });

    it("should refuse a first message that is not hello and a message that is not valid", async () => {
        const notHello = await open();
        const invalid = await open();

        notHello.send({ type: "emit", name: "x" });
        invalid.socket.send("not json");
        const [notHelloError, invalidError] = await Promise.all([
            notHello.next((message) => message["type"] === "error"),
            invalid.next((message) => message["type"] === "error")
        ]);

        expect(notHelloError["code"]).toBe("hello-first");
        expect(typeof invalidError["code"]).toBe("string");
        expect(await notHello.closed).toBe(1008);
        expect(await invalid.closed).toBe(1008);
    });

    it("should refuse an origin that is not allowed and a Host that is not loopback, and accept an allowed origin", async () => {
        const url = (statenet as RunningStatenet).url;
        const port = new URL(url).port;

        const crossOrigin = await upgradeStatus(url, { Origin: "http://evil.example" });
        const rebound = await upgradeStatus(url, { Host: `evil.example:${port}` });
        const allowed = await open({ Origin: "http://127.0.0.1:4100" });
        const wrongPath = await upgradeStatus(url.replace("/sync", "/other"), {});

        expect([crossOrigin, rebound, wrongPath]).toEqual([403, 403, 404]);
        expect(allowed.socket.readyState).toBe(WebSocket.OPEN);
    });

    it("should relay an app server's emit to every other client and a page's emit to the app servers only", async () => {
        // Arrange: an app server (no Origin) and two pages of an allowed origin in one namespace, and one elsewhere
        const page = { Origin: "http://127.0.0.1:4100" };
        const server = await open();
        const first = await open(page);
        const second = await open(page);
        const elsewhere = await open();
        await hello(server, "apps.orders", 0, "server");
        await hello(first, "apps.orders", 0, "first");
        await hello(second, "apps.orders", 0, "second");
        await hello(elsewhere, "apps.other", 0, "elsewhere");
        // Act
        server.send({ type: "emit", name: "stateui.server", value: { text: "to the pages" } });
        first.send({ type: "emit", name: "stateui.view", value: { text: "to the server" } });
        const [toFirst, toSecond, toServer] = await Promise.all([
            first.next((message) => message["type"] === "emit"),
            second.next((message) => message["type"] === "emit"),
            server.next((message) => message["type"] === "emit")
        ]);
        await new Promise((resolve) => setTimeout(resolve, 100));
        // Assert
        expect(toFirst).toEqual({ type: "emit", name: "stateui.server", value: { text: "to the pages" } });
        expect(toSecond).toEqual(toFirst);
        expect(toServer).toEqual({ type: "emit", name: "stateui.view", value: { text: "to the server" } });
        expect(second.received.filter((message) => message["name"] === "stateui.view")).toEqual([]);
        expect(elsewhere.received.filter((message) => message["type"] === "emit")).toEqual([]);
    });
    it("should tell a namespace when one of its clients leaves", async () => {
        const server = await open();
        const page = await open({ Origin: "http://127.0.0.1:4100" });
        await hello(server, "apps.orders", 0, "server");
        await hello(page, "apps.orders", 0, "page-view");
        page.socket.close();
        const left = await server.next((message) => message["type"] === "emit");
        expect(left).toEqual({ type: "emit", name: LEFT_EMIT, value: { clientId: "page-view" } });
    });
    it("should report namespaces, clients and revisions under statenet.service and state-sync.authority, but never change values", async () => {
        // Arrange
        const lines: string[] = [];
        const listener = (_timestamp: number, category: string, message: string, details?: unknown): void => {
            lines.push(`${category} ${message} ${JSON.stringify(details ?? null)}`);
        };
        setCategories(["statenet.*", "state-sync.*", "warning", "error"]);
        addListener(listener, ["statenet.*", "state-sync.*", "warning", "error"]);
        const client = await open();

        // Act
        try {
            await hello(client, "apps.secret", 0, "c");
            client.send({ type: "change", change: { id: "c1", path: "$.data.password", op: "set", value: "hunter2-value", base: 0 } });
            await client.next((message) => message["type"] === "ack");
            await new Promise((resolve) => setTimeout(resolve, 50));
        } finally {
            removeListener(listener, ["statenet.*", "state-sync.*", "warning", "error"]);
            setCategories(["info", "error", "stack"]);
        }

        // Assert
        expect(lines.some((line) => /^statenet\.service Loaded namespace apps\.secret at revision 0 in [\d.]+ ms /.test(line))).toBe(true);
        expect(lines.some((line) => line.startsWith("statenet.service Client c joined apps.secret from an app server "))).toBe(true);
        expect(lines.some((line) => line.startsWith("state-sync.authority Applied the set of client c at $.data.password in apps.secret at revision 1 "))).toBe(true);
        expect(lines.some((line) => line.includes("hunter2-value"))).toBe(false);
    });
});

describe("startStatenet on a host that is not loopback", () => {
    it("should refuse to start", async () => {
        await expect(startStatenet({ host: "0.0.0.0", port: 0, dataDir: "unused" })).rejects.toThrow("loopback only");
        await expect(startStatenet({ host: "192.168.1.10", port: 0, dataDir: "unused" })).rejects.toThrow("loopback only");
    });
});
