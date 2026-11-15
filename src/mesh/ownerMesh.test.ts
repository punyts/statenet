import { createStore } from '@punyts/state';
import { createStoreBridge, createSyncClient } from '@punyts/state-sync';
import type { Change, JsonValue, SyncTransport } from '@punyts/state-sync';
import { createAuthority, createMemoryEpochStorage, createMemoryOwnershipStore } from '@punyts/state-sync/authority';
import type { Authority, AuthoritySession, NamespaceHost, OwnershipStore, RelayListener, WriteOutcome } from '@punyts/state-sync/authority';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { generateKeyPair, issueNodeCertificate } from './certificates.js';
import type { KeyPair } from './certificates.js';
import { startOwnerMesh } from './OwnerMesh.js';
import type { OwnerMesh } from './OwnerMesh.js';
import type { DirectoryView } from './protocol.js';

const HOUR = 3_600_000;
const ADDRESS = 'alice/notes/user';
const NAMESPACE = 'alice_notes_user';
const ca = generateKeyPair();
const closers: Array<() => Promise<void> | void> = [];

afterEach(async () => {
    for (const close of closers.splice(0).reverse()) await close();
});

let counter = 0;
const change = (path: string, value: unknown): Change => ({ id: `o${++counter}`, path, op: 'set', value: value as never, base: 0 });

/** Authorities over one shared storage, as app servers over one database; each records the emits its clients send */
function hostOver(storage: ReturnType<typeof createMemoryEpochStorage>) {
    const running = new Map<string, Promise<Authority>>();
    const emits: string[] = [];
    const host: NamespaceHost = {
        authority: (address, epoch) => {
            let found = running.get(address);
            if (found === undefined) {
                found = createAuthority({ namespace: NAMESPACE, storage: { load: () => storage.load(address), save: (_namespace, stored) => storage.save(address, stored, epoch) }, initial: { data: {} }, createParents: true }).then((made) => {
                    made.onEmit((name) => emits.push(name));
                    return made;
                });
                running.set(address, found);
            }
            return found;
        },
        release: async (address) => {
            const found = running.get(address);
            running.delete(address);
            (await found)?.close();
        }
    };
    return { host, emits, running };
}

/** Two or three owner nodes over one ownership store and storage, linked as the directory names them */
async function cluster(ids: readonly string[], leaseMs = 10_000) {
    const ownership: OwnershipStore = createMemoryOwnershipStore();
    const storage = createMemoryEpochStorage();
    const endpoints = new Map<string, string>();
    const directory = (): DirectoryView => ({ nodes: [...endpoints].map(([id, endpoint]) => ({ id, role: 'owner' as const, region: '', endpoint })), owners: {}, keys: {} });
    const nodes = new Map<string, OwnerMesh & { emits: string[] }>();
    for (const id of ids) {
        const keys: KeyPair = generateKeyPair();
        const { host, emits } = hostOver(storage);
        const mesh = await startOwnerMesh({
            nodeId: id,
            keys,
            certificate: issueNodeCertificate({ nodeId: id, role: 'owner', region: '', publicKey: keys.publicKey, expires: Date.now() + HOUR }, ca.privateKey),
            authorityPublicKey: ca.publicKey,
            ownership,
            host,
            directory,
            leaseMs,
            writeTimeoutMs: 2_000
        });
        endpoints.set(id, mesh.url);
        nodes.set(id, Object.assign(mesh, { emits }));
        closers.push(() => mesh.close());
    }
    return { nodes: (id: string) => nodes.get(id) as OwnerMesh & { emits: string[] }, endpoints, ownership, storage };
}

/** A view: a sync client whose connection is a session a node gives it, here or tunnelled to the owner */
function view(node: OwnerMesh) {
    const store = createStore({});
    const emitted: string[] = [];
    let opened = 0;
    const transport: SyncTransport = {
        connect: (handlers) => {
            let session: AuthoritySession | undefined;
            const waiting: string[] = [];
            let gone = false;
            opened++;
            void node
                .connect(ADDRESS, { send: (text) => handlers.onMessage(text), close: () => !gone && ((gone = true), handlers.onClose()) }, undefined, async () => (await node.node.authorityOf(ADDRESS)) as Authority)
                .then((made) => {
                    session = made;
                    handlers.onOpen();
                    for (const text of waiting.splice(0)) made.receive(text);
                }, () => !gone && ((gone = true), handlers.onClose()));
            return {
                send: (text) => (session === undefined ? waiting.push(text) : session.receive(text)),
                close: () => {
                    gone = true;
                    session?.close();
                }
            };
        }
    };
    const client = createSyncClient({ namespace: NAMESPACE, bridge: createStoreBridge(store), transport, backoff: () => 20, onEmit: (name) => emitted.push(name) });
    client.start();
    closers.push(() => client.stop());
    return { store, client, emitted, opened: () => opened };
}

describe('owner nodes over the mesh', () => {
    it('should forward a write to the node that owns the namespace, where it is applied once', async () => {
        // Arrange
        const { nodes } = await cluster(['a', 'b']);
        await nodes('a').node.write(ADDRESS, change('$.data.first', 1));

        // Act
        const outcome = await nodes('b').node.write(ADDRESS, change('$.data.second', 2));

        // Assert
        expect(outcome).toMatchObject({ result: 'applied', epoch: 1 });
        expect(nodes('b').node.owned()).toEqual([]);
        expect((await nodes('a').node.authorityOf(ADDRESS))?.get('$.data')).toEqual({ first: 1, second: 2 });
    });

    it('should tunnel a view at a node that does not own the namespace to the owner, with its writes and emits both ways', async () => {
        // Arrange
        const { nodes } = await cluster(['a', 'b']);
        await nodes('a').node.write(ADDRESS, change('$.data.title', 'from a'));
        const atB = view(nodes('b'));
        await vi.waitFor(() => expect(atB.client.status().status).toBe('online'));

        // Act
        atB.store.set('$.data.fromB', true);
        atB.client.emit('pressed');
        (await nodes('a').node.authorityOf(ADDRESS))?.emit('notice');

        // Assert
        await vi.waitFor(() => expect((nodes('a').node.owned())).toEqual([ADDRESS]));
        await vi.waitFor(async () => expect((await nodes('a').node.authorityOf(ADDRESS))?.get('$.data.fromB')).toBe(true));
        await vi.waitFor(() => expect(atB.store.get('$.data')).toEqual({ title: 'from a', fromB: true }));
        await vi.waitFor(() => expect(nodes('a').emits).toContain('pressed'));
        await vi.waitFor(() => expect(atB.emitted).toContain('notice'));
        expect(nodes('b').node.owned()).toEqual([]);
    });

    it('should move a namespace by a claim over the wire, landing the writes sent meanwhile after it, and reconnect the views it moved from', async () => {
        // Arrange
        const { nodes, ownership } = await cluster(['a', 'b']);
        await nodes('a').node.write(ADDRESS, change('$.data.log', {}));
        const atA = view(nodes('a'));
        await vi.waitFor(() => expect(atA.client.status().status).toBe('online'));

        // Act: writes through both nodes while b claims it
        const outcomes: Array<Promise<WriteOutcome>> = [];
        let claimed: Promise<boolean> | undefined;
        for (let index = 0; index < 20; index++) {
            outcomes.push(nodes(index % 2 === 0 ? 'a' : 'b').node.write(ADDRESS, change(`$.data.log.k${index}`, index)));
            if (index === 8) claimed = nodes('b').node.claim(ADDRESS);
        }
        const results = await Promise.all(outcomes);

        // Assert
        expect(await claimed).toBe(true);
        expect(results.every((result) => result.result === 'applied')).toBe(true);
        expect(nodes('b').node.owned()).toEqual([ADDRESS]);
        expect(nodes('a').node.owned()).toEqual([]);
        expect((await ownership.get(ADDRESS))?.epoch).toBe(2);
        const log = (await nodes('b').node.authorityOf(ADDRESS))?.get('$.data.log') as Record<string, number>;
        expect(Object.keys(log)).toHaveLength(20);
        // the view at a was closed with a's authority, and came back tunnelled to b
        await vi.waitFor(() => expect(atA.opened()).toBeGreaterThan(1));
        await vi.waitFor(() => expect(Object.keys((atA.store.get('$.data.log') ?? {}) as object)).toHaveLength(20));
        atA.store.set('$.data.after', 'moved');
        await vi.waitFor(async () => expect((await nodes('b').node.authorityOf(ADDRESS))?.get('$.data.after')).toBe('moved'));
    });

    it('should keep a relay subscription at a node following the namespace as it moves between nodes', async () => {
        // Arrange
        const { nodes } = await cluster(['a', 'b', 'c']);
        await nodes('a').node.write(ADDRESS, change('$.data.n', 0));
        const seen: { value: JsonValue | undefined } = { value: undefined };
        const listener: RelayListener = {
            welcome: (welcome) => {
                if ('value' in welcome) seen.value = structuredClone(welcome.value);
            },
            changes: (batch) => {
                for (const one of batch.changes) if (one.path === '$.data.n') (seen.value as { data: { n: unknown } }).data.n = one.value;
            }
        };
        nodes('c').relay.subscribe(ADDRESS, '$', undefined, listener, { subject: 'c' });
        await vi.waitFor(() => expect(seen.value).toEqual({ data: { n: 0 } }));

        // Act
        await nodes('a').node.write(ADDRESS, change('$.data.n', 1));
        await vi.waitFor(() => expect(seen.value).toEqual({ data: { n: 1 } }));
        expect(await nodes('b').node.claim(ADDRESS)).toBe(true);
        await nodes('b').node.write(ADDRESS, change('$.data.n', 2));

        // Assert
        await vi.waitFor(() => expect(seen.value).toEqual({ data: { n: 2 } }));
        expect(nodes('c').node.owned()).toEqual([]);
    });

    it('should take over the namespace of a node that is gone once its lease runs out', async () => {
        // Arrange
        const { nodes, endpoints } = await cluster(['a', 'b'], 300);
        await nodes('a').node.write(ADDRESS, change('$.data.before', 1));
        await (await nodes('a').node.authorityOf(ADDRESS))?.flush();

        // Act: a goes, and the directory stops naming it
        await nodes('a').close();
        endpoints.delete('a');
        await new Promise((resolve) => setTimeout(resolve, 350));
        const outcome = await nodes('b').node.write(ADDRESS, change('$.data.after', 2));

        // Assert
        expect(outcome).toMatchObject({ result: 'applied', epoch: 2 });
        expect((await nodes('b').node.authorityOf(ADDRESS))?.get('$.data')).toEqual({ before: 1, after: 2 });
    });
});
