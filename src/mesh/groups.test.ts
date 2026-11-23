import { createServer } from 'node:net';
import { createStore } from '@punyts/state';
import { createStoreBridge, createSyncClient } from '@punyts/state-sync';
import type { Change, JsonValue, SyncTransport } from '@punyts/state-sync';
import { createAuthority, createMemoryEpochStorage, createMemoryOwnershipStore, generateGrantKey, signGrant } from '@punyts/state-sync/authority';
import type { Authority, AuthoritySession, EpochStorage, GrantContext, NamespaceHost, OwnershipStore, RelayListener } from '@punyts/state-sync/authority';
import { addListener, removeListener, setCategories } from '@punyts/core';
import type { ListenerFn } from '@punyts/core';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createAggregateStorage, startAggregateNode } from './Aggregate.js';
import { generateKeyPair, issueNodeCertificate } from './certificates.js';
import type { NodeRole } from './certificates.js';
import { createMeshUpstream } from './MeshUpstream.js';
import { startOwnerMesh } from './OwnerMesh.js';
import type { OwnerMesh } from './OwnerMesh.js';
import type { DirectoryView } from './protocol.js';
import { startRelayNode } from './RelayNode.js';
import type { RelayNode } from './RelayNode.js';

const HOUR = 3_600_000;
const ADDRESS = 'alice/notes/user';
const NAMESPACE = 'alice.notes.user';
const ca = generateKeyPair();
const closers: Array<() => Promise<void> | void> = [];

afterEach(async () => {
    for (const close of closers.splice(0).reverse()) await close();
});

let counter = 0;
const change = (path: string, value: unknown): Change => ({ id: `g${++counter}`, path, op: 'set', value: value as never, base: 0 });

const certificate = (nodeId: string, role: NodeRole, publicKey: string, group?: string, endpoint?: string): string =>
    issueNodeCertificate({ nodeId, role, region: '', publicKey, expires: Date.now() + HOUR, ...(group === undefined ? {} : { group }), ...(endpoint === undefined ? {} : { endpoint }) }, ca.privateKey);

/** A port free a moment ago, for a relay whose certificate must name its endpoint before it starts */
const freePort = (): Promise<number> =>
    new Promise((resolve, reject) => {
        const server = createServer();
        server.once('error', reject);
        server.listen(0, '127.0.0.1', () => {
            const { port } = server.address() as { port: number };
            server.close(() => resolve(port));
        });
    });

/** A node's authorities over a storage: one shared by the servers, or each node's way to the aggregate; `access` checks writes */
function hostOver(storageOf: () => EpochStorage, access?: (address: string) => GrantContext) {
    const running = new Map<string, Promise<Authority>>();
    const host: NamespaceHost = {
        authority: (address, epoch) => {
            let found = running.get(address);
            if (found === undefined) {
                const storage = storageOf();
                found = createAuthority({ namespace: NAMESPACE, storage: { load: () => storage.load(address), save: (_namespace, stored) => storage.save(address, stored, epoch) }, initial: { data: {} }, createParents: true, ...(access === undefined ? {} : { access: { address, context: () => access(address) } }) });
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
    return host;
}

interface Placed {
    readonly id: string;
    readonly group: string;
    readonly relay: string;
}

/**
 * An aggregate, a relay per group linked to it, and nodes each linked up to its group's relay: nodes of one group also
 * link to each other. `throughAggregate` has the nodes save through their relay to the aggregate; otherwise they share
 * a storage, as servers on one database do. `selfListed` writes no row by hand: the aggregate lists the relays linked to
 * it from their certificates, which name their endpoints, and each node lists the relay its link reached as its parent.
 * `under` links a relay to another relay instead of the aggregate (relays under relays; the parent is placed first).
 * `access` has every node's authorities check writes against the access they came with
 */
async function statenet(placed: readonly Placed[], throughAggregate = false, selfListed = false, under: Readonly<Record<string, string>> = {}, access?: (address: string) => GrantContext) {
    const ownership: OwnershipStore = createMemoryOwnershipStore();
    const database = createMemoryEpochStorage();
    const entries: Array<DirectoryView['nodes'][number]> = [];
    const directory = (): DirectoryView => ({ nodes: entries, owners: {}, keys: {} });
    const aggregateKeys = generateKeyPair();
    const unlist = (id: string): void => {
        const at = entries.findIndex((entry) => entry.id === id);
        if (at >= 0) entries.splice(at, 1);
    };
    const relayListing = {
        list: async (relay: { id: string; region: string; group: string; endpoint: string; publicKey: string; parent: string }) => {
            unlist(relay.id);
            entries.push({ id: relay.id, role: 'relay', region: relay.region, endpoint: relay.endpoint, group: relay.group, parent: relay.parent, publicKey: relay.publicKey });
        },
        unlist: async (id: string) => unlist(id)
    };
    const aggregatePort = await freePort();
    const startAggregate = () => startAggregateNode({ nodeId: 'aggregate', keys: aggregateKeys, certificate: certificate('aggregate', 'aggregate', aggregateKeys.publicKey), authorityPublicKey: ca.publicKey, storage: database, directory, port: aggregatePort, ...(selfListed ? { relays: relayListing } : {}) });
    let aggregate = await startAggregate();
    closers.push(() => aggregate.close());
    const relays = new Map<string, RelayNode>();
    for (const id of new Set(placed.map((node) => node.relay))) {
        const keys = generateKeyPair();
        const port = selfListed ? await freePort() : 0;
        const relayCertificate = selfListed ? certificate(id, 'relay', keys.publicKey, undefined, `ws://127.0.0.1:${port}`) : certificate(id, 'relay', keys.publicKey);
        const relay = await startRelayNode({ nodeId: id, keys, certificate: relayCertificate, authorityPublicKey: ca.publicKey, parents: [under[id] === undefined ? aggregate.url : (relays.get(under[id]) as RelayNode).meshUrl], checkGrants: false, meshPort: port, upstream: { renewWithinMs: 0, followDirectory: false, backoff: () => 50 } });
        relays.set(id, relay);
        if (!selfListed) entries.push({ id, role: 'relay', region: '', endpoint: relay.meshUrl });
        closers.push(() => relay.close());
    }
    const nodes = new Map<string, OwnerMesh>();
    for (const { id, group, relay } of placed) {
        const keys = generateKeyPair();
        let mesh: OwnerMesh | undefined;
        const host = hostOver(() => (throughAggregate && mesh?.uplink !== undefined ? createAggregateStorage(mesh.uplink) : database), access);
        mesh = await startOwnerMesh({
            nodeId: id,
            group,
            keys,
            certificate: certificate(id, 'owner', keys.publicKey, group),
            authorityPublicKey: ca.publicKey,
            ownership,
            host,
            directory,
            relays: [(relays.get(relay) as RelayNode).meshUrl],
            ...(selfListed
                ? {
                      onUplinked: (parent: string) => {
                          unlist(id);
                          entries.push({ id, role: 'owner', region: '', endpoint: (mesh as OwnerMesh).url, group, parent });
                          aggregate.mesh.directoryChanged();
                      }
                  }
                : {}),
            leaseMs: 10_000,
            writeTimeoutMs: 3_000
        });
        const made = mesh;
        if (!selfListed) entries.push({ id, role: 'owner', region: '', endpoint: made.url, group, parent: relay });
        nodes.set(id, made);
        closers.push(() => made.close());
    }
    // every relay knows the nodes and where each is linked, from the aggregate's directory
    aggregate.mesh.directoryChanged();
    await vi.waitFor(() => {
        for (const node of nodes.values()) expect(node.uplink?.linked()).toBe(true);
        expect(entries).toHaveLength(relays.size + nodes.size);
        aggregate.mesh.directoryChanged();
        for (const relay of relays.values()) expect(relay.upstream.directory()?.nodes).toHaveLength(entries.length);
    });
    return {
        nodes: (id: string) => nodes.get(id) as OwnerMesh,
        relays: (id: string) => relays.get(id) as RelayNode,
        aggregate: () => aggregate,
        /** The aggregate stops, closing every relay's link to it */
        stopAggregate: () => aggregate.close(),
        /** A new aggregate at the same address: the relays link to it again */
        startAggregate: async () => {
            aggregate = await startAggregate();
        },
        directory: entries,
        database,
        ownership
    };
}

/** A view: a sync client whose connection is a session a node gives it, here or tunnelled to the owner */
function view(node: OwnerMesh) {
    const store = createStore({});
    const transport: SyncTransport = {
        connect: (handlers) => {
            let session: AuthoritySession | undefined;
            const waiting: string[] = [];
            let gone = false;
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
    const client = createSyncClient({ namespace: NAMESPACE, bridge: createStoreBridge(store), transport, backoff: () => 20 });
    client.start();
    closers.push(() => client.stop());
    return { store, client };
}

const TWO_GROUPS: readonly Placed[] = [
    { id: 'a1', group: 'a', relay: 'relay-a' },
    { id: 'b1', group: 'b', relay: 'relay-b' }
];

/** The warnings and statenet reports heard during a test, from @punyts/core's reporter */
const heard: string[] = [];
const hear: ListenerFn = (_timestamp, _category, message) => {
    heard.push(message);
};
const HEARD = ['warning', 'statenet.*'];
beforeEach(() => {
    heard.length = 0;
    setCategories(['error', ...HEARD]);
    addListener(hear, HEARD);
});
afterEach(() => {
    removeListener(hear, HEARD);
    setCategories(['info', 'error', 'stack']);
});
/** What was heard of a node: the reports that name it */
const heardOf = (nodeId: string): string[] => heard.filter((line) => line.includes(nodeId));

describe('nodes in groups', () => {
    it('should refuse a node of another group that links to a node directly, at either end', async () => {
        // Arrange
        const { nodes } = await statenet(TWO_GROUPS);
        const keys = generateKeyPair();

        // Act: a node of group a dials b1
        const honest = createMeshUpstream({ nodeId: 'a2', role: 'owner', group: 'a', keys, certificate: certificate('a2', 'owner', keys.publicKey, 'a'), authorityPublicKey: ca.publicKey, parents: [nodes('b1').url], followDirectory: false, backoff: () => 5_000 });
        closers.push(() => honest.close());
        await vi.waitFor(() => expect(heardOf('a2').join(' ')).toContain('does not link'));

        // Assert
        expect(honest.linked()).toBe(false);
        expect(heardOf('a2').join('\n')).toContain('a node of group a does not link to a node of group b');
        expect(nodes('b1').mesh.stats().links).toEqual([]);
    });

    it('should link the nodes of one group to each other directly', async () => {
        // Arrange
        const { nodes } = await statenet([
            { id: 'a1', group: 'a', relay: 'relay-a' },
            { id: 'a2', group: 'a', relay: 'relay-a' }
        ]);
        await nodes('a1').node.write(ADDRESS, change('$.data.first', 1));

        // Act
        const outcome = await nodes('a2').node.write(ADDRESS, change('$.data.second', 2));

        // Assert
        expect(outcome).toMatchObject({ result: 'applied' });
        expect(nodes('a1').mesh.stats().links.map((link) => link.nodeId)).toEqual(['a2']);
        expect((await nodes('a1').node.authorityOf(ADDRESS))?.get('$.data')).toEqual({ first: 1, second: 2 });
    });

    it('should carry a write to a node of another group through their relays, where it is applied once', async () => {
        // Arrange
        const { nodes, relays } = await statenet(TWO_GROUPS);
        await nodes('b1').node.write(ADDRESS, change('$.data.first', 1));

        // Act
        const outcome = await nodes('a1').node.write(ADDRESS, change('$.data.second', 2));

        // Assert
        expect(outcome).toMatchObject({ result: 'applied', epoch: 1 });
        expect(nodes('a1').node.owned()).toEqual([]);
        expect((await nodes('b1').node.authorityOf(ADDRESS))?.get('$.data')).toEqual({ first: 1, second: 2 });
        expect(nodes('b1').mesh.stats().links).toEqual([]);
        expect(relays('relay-b').mesh.stats().links.map((link) => link.nodeId).sort()).toEqual(['b1', 'relay-a']);
    });

    it('should carry a write to a node of another group with a directory the aggregate and the nodes wrote themselves', async () => {
        // Arrange
        const { nodes, aggregate } = await statenet(TWO_GROUPS, false, true);
        await nodes('b1').node.write(ADDRESS, change('$.data.first', 1));

        // Act
        const outcome = await nodes('a1').node.write(ADDRESS, change('$.data.second', 2));

        // Assert
        expect(outcome).toMatchObject({ result: 'applied', epoch: 1 });
        expect((await nodes('b1').node.authorityOf(ADDRESS))?.get('$.data')).toEqual({ first: 1, second: 2 });
        expect(aggregate().relays().map((relay) => relay.id).sort()).toEqual(['relay-a', 'relay-b']);
    });

    it('should tunnel a view to the owner in another group through the relays, with its writes', async () => {
        // Arrange
        const { nodes } = await statenet(TWO_GROUPS);
        await nodes('b1').node.write(ADDRESS, change('$.data.title', 'from b'));
        const atA = view(nodes('a1'));
        await vi.waitFor(() => expect(atA.client.status().status).toBe('online'));

        // Act
        atA.store.set('$.data.fromA', true);

        // Assert
        await vi.waitFor(async () => expect((await nodes('b1').node.authorityOf(ADDRESS))?.get('$.data')).toEqual({ title: 'from b', fromA: true }));
        expect(atA.store.get('$.data.title')).toBe('from b');
        expect(nodes('b1').node.owned()).toEqual([ADDRESS]);
    });

    it('should serve a subscription to a namespace another group owns through the relays', async () => {
        // Arrange
        const { nodes } = await statenet(TWO_GROUPS);
        await nodes('b1').node.write(ADDRESS, change('$.data.n', 0));
        const seen: { value: JsonValue | undefined } = { value: undefined };
        const listener: RelayListener = {
            welcome: (welcome) => {
                if ('value' in welcome) seen.value = structuredClone(welcome.value);
            },
            changes: (batch) => {
                for (const one of batch.changes) if (one.path === '$.data.n') (seen.value as { data: { n: unknown } }).data.n = one.value;
            }
        };

        // Act
        nodes('a1').relay.subscribe(ADDRESS, '$', undefined, listener, { subject: 'a1' });
        await vi.waitFor(() => expect(seen.value).toEqual({ data: { n: 0 } }));
        await nodes('b1').node.write(ADDRESS, change('$.data.n', 1));

        // Assert
        await vi.waitFor(() => expect(seen.value).toEqual({ data: { n: 1 } }));
    });

    it('should move a namespace to a node of another group by a claim through the relays', async () => {
        // Arrange
        const { nodes, ownership } = await statenet(TWO_GROUPS);
        await nodes('b1').node.write(ADDRESS, change('$.data.n', 1));

        // Act
        const claimed = await nodes('a1').node.claim(ADDRESS);
        const after = await nodes('b1').node.write(ADDRESS, change('$.data.m', 2));

        // Assert
        expect(claimed).toBe(true);
        expect((await ownership.get(ADDRESS))?.node).toBe('a1');
        expect(after).toMatchObject({ result: 'applied' });
        expect(nodes('a1').node.owned()).toEqual([ADDRESS]);
        expect((await nodes('a1').node.authorityOf(ADDRESS))?.get('$.data')).toEqual({ n: 1, m: 2 });
    });

    it('should reject a write for a node the relays know no way to', async () => {
        // Arrange
        const { nodes } = await statenet(TWO_GROUPS);

        // Act
        const outcome = await nodes('a1').uplink?.write(ADDRESS, change('$.data.n', 1), undefined, 'nowhere');

        // Assert
        expect(outcome).toEqual({ result: 'rejected', reason: 'node nowhere cannot be reached from relay-a' });
    });

    it('should save what a node orders through its relay to the aggregate, and load it at the node that takes it over', async () => {
        // Arrange
        const { nodes, database, aggregate } = await statenet(TWO_GROUPS, true);

        // Act
        await nodes('a1').node.write(ADDRESS, change('$.data.n', 1));
        await (await nodes('a1').node.authorityOf(ADDRESS))?.whenDurable();
        const claimed = await nodes('b1').node.claim(ADDRESS);

        // Assert
        expect((await database.load(ADDRESS))?.state).toEqual({ data: { n: 1 } });
        expect(aggregate().aggregate.stats().saves).toBeGreaterThan(0);
        expect(claimed).toBe(true);
        expect((await nodes('b1').node.authorityOf(ADDRESS))?.get('$.data')).toEqual({ n: 1 });
    });
});

describe('relays under relays', () => {
    /** Relay b is linked to relay a, relay a to the aggregate; a node of each group under each relay */
    const CHAIN: readonly Placed[] = [
        { id: 'a1', group: 'a', relay: 'relay-a' },
        { id: 'b1', group: 'b', relay: 'relay-b' }
    ];
    const parents = (directory: ReadonlyArray<{ id: string; role: string; parent?: string }>): Array<[string, string | undefined]> =>
        directory
            .filter((entry) => entry.role === 'relay')
            .map((entry): [string, string | undefined] => [entry.id, entry.parent])
            .sort();

    it('should list a relay under a relay with its parent, and carry a write, a subscription and a claim between the nodes below them', async () => {
        // Arrange
        const { nodes, aggregate, directory, ownership } = await statenet(CHAIN, false, true, { 'relay-b': 'relay-a' });
        await nodes('b1').node.write(ADDRESS, change('$.data.n', 0));
        const seen: { value: JsonValue | undefined } = { value: undefined };
        const listener: RelayListener = {
            welcome: (welcome) => {
                if ('value' in welcome) seen.value = structuredClone(welcome.value);
            },
            changes: (batch) => {
                for (const one of batch.changes) if (one.path === '$.data.n') (seen.value as { data: { n: unknown } }).data.n = one.value;
            }
        };

        // Act
        const written = await nodes('a1').node.write(ADDRESS, change('$.data.fromA', true));
        nodes('a1').relay.subscribe(ADDRESS, '$', undefined, listener, { subject: 'a1' });
        await vi.waitFor(() => expect(seen.value).toMatchObject({ data: { n: 0 } }));
        await nodes('b1').node.write(ADDRESS, change('$.data.n', 1));
        await vi.waitFor(() => expect(seen.value).toMatchObject({ data: { n: 1 } }));
        const claimed = await nodes('a1').node.claim(ADDRESS);

        // Assert
        expect(parents(directory)).toEqual([
            ['relay-a', 'aggregate'],
            ['relay-b', 'relay-a']
        ]);
        expect(aggregate().relays().map((relay) => [relay.id, relay.parent])).toEqual([
            ['relay-a', 'aggregate'],
            ['relay-b', 'relay-a']
        ]);
        expect(written).toMatchObject({ result: 'applied' });
        expect(claimed).toBe(true);
        expect((await ownership.get(ADDRESS))?.node).toBe('a1');
        expect((await nodes('a1').node.authorityOf(ADDRESS))?.get('$.data')).toEqual({ n: 1, fromA: true });
    });

    it('should unlist the relay under a relay when that relay loses its link to the aggregate, and list it again when the link comes back', async () => {
        // Arrange
        const { directory, stopAggregate, startAggregate, relays } = await statenet(CHAIN, false, true, { 'relay-b': 'relay-a' });

        // Act
        await stopAggregate();
        await vi.waitFor(() => expect(parents(directory)).toEqual([]));
        await startAggregate();

        // Assert
        await vi.waitFor(() =>
            expect(parents(directory)).toEqual([
                ['relay-a', 'aggregate'],
                ['relay-b', 'relay-a']
            ])
        );
        expect(relays('relay-b').upstream.parentId()).toBe('relay-a');
    });

    it('should list no relay a relay below passes up with a certificate the authority did not sign', async () => {
        // Arrange
        const { aggregate, relays } = await statenet(CHAIN, false, true, { 'relay-b': 'relay-a' });
        const forger = generateKeyPair();
        const forged = issueNodeCertificate({ nodeId: 'relay-x', role: 'relay', region: '', publicKey: forger.publicKey, expires: Date.now() + HOUR }, forger.privateKey);

        // Act
        relays('relay-a').upstream.relayLinked('relay-x', forged, 'relay-a');
        await vi.waitFor(() => expect(heardOf('relay-x').concat(heard.filter((line) => line.includes('lists no relay'))).length).toBeGreaterThan(0));

        // Assert
        expect(aggregate().relays().map((relay) => relay.id)).toEqual(['relay-a', 'relay-b']);
    });
});

describe('requests across groups', () => {
    it('should keep a write from a node of another group as a request when its sender may only request, until the owner approves it', async () => {
        // Arrange
        const alice = generateGrantKey();
        const now = Date.now();
        const context = (): GrantContext => ({ keyOf: (kid) => (kid === 'alice-1' ? { ownerId: 'alice', publicKey: alice.publicKey } : undefined), now: Date.now() });
        const { nodes } = await statenet(TWO_GROUPS, false, false, {}, context);
        const grant = signGrant({ kid: 'alice-1', issuer: 'alice', subject: 'bob', owner: 'alice', pattern: '$.notes.user.data', rights: ['request'], issued: now, expires: now + HOUR }, alice.privateKey);
        await nodes('a1').node.write(ADDRESS, change('$.data.title', 'Mine'), 'a1', { subject: 'alice' });
        const asked = change('$.data.title', 'Suggested');

        // Act
        const outcome = await nodes('b1').node.write(ADDRESS, asked, 'b1', { subject: 'bob', grant });
        const authority = (await nodes('a1').node.authorityOf(ADDRESS)) as Authority;
        const before = authority.get('$.data.title');
        const decided = authority.decide({ id: asked.id, action: 'approve' }, { subject: 'alice' });

        // Assert
        expect(outcome).toEqual({ result: 'requested', epoch: 1 });
        expect(before).toBe('Mine');
        expect(decided).toEqual({ ok: true, status: 'approved' });
        expect(authority.get('$.data.title')).toBe('Suggested');
        expect(authority.requests()).toMatchObject([{ by: 'bob', status: 'approved', decidedBy: 'alice' }]);
    });
});
