import { createStore } from '@punyts/state';
import { createStoreBridge, createSyncClient } from '@punyts/state-sync';
import type { Change, JsonValue, RevisedChange, SyncTransport } from '@punyts/state-sync';
import { createAuthorityUpstream, createMemoryEpochStorage, createMemoryOwnershipStore, createRelay, generateGrantKey, OwnerNode, signGrant } from '@punyts/state-sync/authority';
import type { RelayListener } from '@punyts/state-sync/authority';
import { addListener, removeListener, setCategories } from '@punyts/core';
import type { ListenerFn } from '@punyts/core';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { generateKeyPair, issueNodeCertificate } from './certificates.js';
import type { KeyPair, NodeRole } from './certificates.js';
import { namespaceOfAddress, startClientServer } from './ClientServer.js';
import { startMeshServer } from './MeshServer.js';
import { createMeshUpstream } from './MeshUpstream.js';
import type { DirectoryView } from './protocol.js';
import { startRelayNode } from './RelayNode.js';

const HOUR = 3_600_000;
const ADDRESS = 'alice/notes/user';
const authority = generateKeyPair();
const closers: Array<() => Promise<void> | void> = [];

afterEach(async () => {
    for (const close of closers.splice(0).reverse()) await close();
});

const certificateFor = (nodeId: string, role: NodeRole, keys: KeyPair, issuer: KeyPair = authority): string =>
    issueNodeCertificate({ nodeId, role, region: '', publicKey: keys.publicKey, expires: Date.now() + HOUR }, issuer.privateKey);

let counter = 0;
const change = (path: string, value: unknown): Change => ({ id: `m${++counter}`, path, op: 'set', value: value as never, base: 0 });

/** An owner node with its mesh server and the browsers' server at it */
async function startOwner(directory: (meshUrl: string) => DirectoryView = (url) => ({ nodes: [{ id: 'owner', role: 'owner', region: '', endpoint: url }], owners: {}, keys: {} }), maxHeld?: number) {
    const keys = generateKeyPair();
    const owner = new OwnerNode({ id: 'owner', ownership: createMemoryOwnershipStore(), storage: createMemoryEpochStorage(), link: () => undefined, initial: () => ({ data: {} }) });
    const local = createRelay(
        createAuthorityUpstream(async (address) => owner.authorityOf(address), (address) => owner.epochOf(address)),
        { sweep: false }
    );
    let url = '';
    const mesh = await startMeshServer({
        nodeId: 'owner',
        role: 'owner',
        keys,
        certificate: certificateFor('owner', 'owner', keys),
        authorityPublicKey: authority.publicKey,
        upstream: local,
        write: (address, written) => owner.write(address, written),
        directory: () => directory(url),
        ...(maxHeld === undefined ? {} : { maxHeld })
    });
    url = mesh.url;
    const clients = await startClientServer({ upstream: local, write: (address, written) => owner.write(address, written) });
    closers.push(() => mesh.close(), () => clients.close(), () => owner.stop(), () => local.close());
    return { owner, mesh, clients };
}

async function relayNode(nodeId: string, parents: readonly string[], options: { clientPort?: number; checkGrants?: boolean } = {}) {
    const keys = generateKeyPair();
    const node = await startRelayNode({ nodeId, keys, certificate: certificateFor(nodeId, 'relay', keys), authorityPublicKey: authority.publicKey, parents, checkGrants: options.checkGrants ?? false, upstream: { backoff: () => 20, silenceMs: 600 }, ...(options.clientPort === undefined ? {} : { clientPort: options.clientPort }) });
    closers.push(() => node.close());
    return node;
}

/** A browser: the sync client over a WebSocket, recording the change ids it is sent in order */
function browser(url: string) {
    const ids: string[] = [];
    const transport: SyncTransport = {
        connect: (handlers) => {
            const socket = new WebSocket(url);
            socket.onopen = () => handlers.onOpen();
            socket.onmessage = (event) => {
                const text = String(event.data);
                const message = JSON.parse(text) as { type: string; changes?: RevisedChange[] };
                if (message.type === 'changes' || message.type === 'welcome') for (const each of message.changes ?? []) ids.push(each.id);
                handlers.onMessage(text);
            };
            socket.onclose = () => handlers.onClose();
            return { send: (text) => socket.send(text), close: () => socket.close() };
        }
    };
    const store = createStore({});
    const client = createSyncClient({ namespace: namespaceOfAddress(ADDRESS), bridge: createStoreBridge(store), transport, backoff: () => 20 });
    client.start();
    closers.push(() => client.stop());
    return { store, client, ids };
}

/** A listener that keeps the value under its prefix and checks it never sees a revision twice */
function follower(prefix: string) {
    const state: { value: JsonValue | undefined; revision: number; repeats: number; seen: number[] } = { value: undefined, revision: 0, repeats: 0, seen: [] };
    const apply = (one: RevisedChange) => {
        if (state.seen.includes(one.revision)) state.repeats++;
        state.seen.push(one.revision);
        const rest = one.path === prefix ? [] : one.path.slice(prefix.length + 1).split('.');
        if (rest.length === 0) {
            state.value = one.op === 'delete' ? undefined : structuredClone(one.value);
            return;
        }
        let node = (state.value ??= {}) as { [key: string]: JsonValue };
        for (const step of rest.slice(0, -1)) node = (node[step] ??= {}) as { [key: string]: JsonValue };
        const last = rest[rest.length - 1] as string;
        if (one.op === 'delete') delete node[last];
        else node[last] = structuredClone(one.value as JsonValue);
    };
    const listener: RelayListener = {
        welcome: (welcome) => {
            if ('value' in welcome) state.value = structuredClone(welcome.value);
            else for (const one of welcome.changes) apply(one);
            state.revision = welcome.revision;
        },
        changes: (batch) => {
            for (const one of batch.changes) apply(one);
            state.revision = batch.revision;
        }
    };
    return { listener, state };
}

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

describe('the mesh', () => {
    it('should give a browser behind a regional relay the same changes, in the same order, as one at the server', async () => {
        // Arrange: owner, a core relay, and a regional relay below it
        const { owner, clients } = await startOwner();
        await owner.write(ADDRESS, change('$.data.n', 0));
        const core = await relayNode('relay-core', [(await startOwnerUrl(owner))]);
        const regional = await relayNode('relay-eu', [core.meshUrl], { clientPort: 0 });
        const atServer = browser(clients.url);
        const atEdge = browser(regional.clientUrl as string);
        await vi.waitFor(() => expect([atServer.client.status().status, atEdge.client.status().status]).toEqual(['online', 'online']));

        // Act: writes at the owner, and one from the browser at the edge, which goes up the mesh
        for (let index = 1; index <= 30; index++) await owner.write(ADDRESS, change('$.data.n', index));
        atEdge.store.set('$.data.fromEdge', true);
        for (let index = 31; index <= 40; index++) await owner.write(ADDRESS, change('$.data.n', index));

        // Assert
        await vi.waitFor(() => {
            expect(atServer.store.get('$')).toEqual({ data: { n: 40, fromEdge: true } });
            expect(atEdge.store.get('$')).toEqual({ data: { n: 40, fromEdge: true } });
        });
        const fromEdge = atServer.ids.find((id) => !id.startsWith('m')) as string;
        const tail = (ids: string[]) => ids.filter((id) => id !== fromEdge);
        expect(tail(atEdge.ids)).toEqual(tail(atServer.ids));
        expect((await owner.authorityOf(ADDRESS))?.get('$.data.fromEdge')).toBe(true);
    });

    it('should refuse a link from a node without a certificate from the authority', async () => {
        // Arrange
        const { owner, mesh } = await startOwner();
        await owner.write(ADDRESS, change('$.data.n', 0));
        const stranger = generateKeyPair();
        const otherAuthority = generateKeyPair();
        const forged = createMeshUpstream({ nodeId: 'forged', role: 'relay', keys: stranger, certificate: certificateFor('forged', 'relay', stranger, otherAuthority), authorityPublicKey: authority.publicKey, parents: [mesh.url], backoff: () => 50 });
        const none = createMeshUpstream({ nodeId: 'none', role: 'relay', keys: stranger, certificate: '', authorityPublicKey: authority.publicKey, parents: [mesh.url], backoff: () => 50 });
        closers.push(() => forged.close(), () => none.close());

        // Act
        await new Promise((resolve) => setTimeout(resolve, 200));

        // Assert
        expect(forged.linked()).toBe(false);
        expect(none.linked()).toBe(false);
        expect(mesh.stats().links).toEqual([]);
        expect(heardOf('forged').some((line) => line.includes("not signed by Magnetic's certificate authority"))).toBe(true);
    });

    it('should let a slow node lag and catch up without the owner node holding more than its limit for it', async () => {
        // Arrange: a node below that grants no credit until it is ready
        const { owner, mesh } = await startOwner(undefined, 50);
        await owner.write(ADDRESS, change('$.data.n', 0));
        const keys = generateKeyPair();
        const slow = createMeshUpstream({ nodeId: 'slow', role: 'relay', keys, certificate: certificateFor('slow', 'relay', keys), authorityPublicKey: authority.publicKey, parents: [mesh.url], manualCredit: true, backoff: () => 20 });
        closers.push(() => slow.close());
        const one = follower('$.data');
        slow.subscribe(ADDRESS, '$.data', undefined, one.listener);
        await vi.waitFor(() => expect(one.state.value).toEqual({ n: 0 }));

        // Act
        let mostHeld = 0;
        for (let index = 1; index <= 300; index++) {
            await owner.write(ADDRESS, change(`$.data.k${index % 120}`, index));
            mostHeld = Math.max(mostHeld, ...mesh.stats().links.map((link) => link.held));
        }
        slow.grant(10_000);

        // Assert
        expect(mostHeld).toBeLessThanOrEqual(51);
        await vi.waitFor(async () => expect(one.state.value).toEqual((await owner.authorityOf(ADDRESS))?.get('$.data')));
        expect(one.state.repeats).toBe(0);
    });

    it('should reconnect a relay whose parent goes away to another, and miss no change', async () => {
        // Arrange: two core relays under the owner, and a regional relay that prefers the first
        const { owner } = await startOwner();
        await owner.write(ADDRESS, change('$.data.n', 0));
        const ownerUrl = await startOwnerUrl(owner);
        const first = await relayNode('relay-core-1', [ownerUrl]);
        const second = await relayNode('relay-core-2', [ownerUrl]);
        const regional = await relayNode('relay-eu', [first.meshUrl, second.meshUrl]);
        const one = follower('$.data');
        regional.relay.subscribe(ADDRESS, '$.data', undefined, one.listener);
        await vi.waitFor(() => expect(one.state.value).toEqual({ n: 0 }));
        for (let index = 1; index <= 10; index++) await owner.write(ADDRESS, change('$.data.n', index));

        // Act: the first core relay goes away while writes go on
        await first.close();
        for (let index = 11; index <= 20; index++) await owner.write(ADDRESS, change('$.data.n', index));
        await vi.waitFor(() => expect(regional.upstream.parent()).toBe(second.meshUrl));
        for (let index = 21; index <= 30; index++) await owner.write(ADDRESS, change('$.data.n', index));

        // Assert
        await vi.waitFor(() => expect(one.state.value).toEqual({ n: 30 }));
        expect(one.state.repeats).toBe(0);
    });

    it('should teach a relay with no database the owners, nodes and keys it needs, and check grants with them', async () => {
        // Arrange: the owner's directory names a key of Alice's, which signs a grant for Bob
        const alice = generateGrantKey();
        const directory = (url: string): DirectoryView => ({
            nodes: [{ id: 'owner', role: 'owner', region: 'eu', endpoint: url }],
            owners: { [ADDRESS]: { node: 'owner', epoch: 1 } },
            keys: { 'alice-1': { ownerId: 'alice', publicKey: alice.publicKey } }
        });
        const { owner } = await startOwner(directory);
        await owner.write(ADDRESS, change('$.data.n', 1));
        const edge = await relayNode('relay-edge', [await startOwnerUrl(owner, directory)], { checkGrants: true });
        await vi.waitFor(() => expect(edge.upstream.directory()).toBeDefined());
        const grant = signGrant({ kid: 'alice-1', issuer: 'alice', subject: 'bob', address: ADDRESS, prefix: '$.data', rights: ['read'], issued: Date.now(), expires: Date.now() + HOUR }, alice.privateKey);
        const bob = follower('$.data');
        const carol = { ...follower('$.data'), closed: '' };

        // Act
        edge.relay.subscribe(ADDRESS, '$.data', undefined, bob.listener, { subject: 'bob', grant });
        edge.relay.subscribe(ADDRESS, '$.data', undefined, { ...carol.listener, closed: (reason) => (carol.closed = reason) }, { subject: 'carol' });

        // Assert
        expect(edge.upstream.directory()).toMatchObject({ owners: { [ADDRESS]: { node: 'owner' } }, nodes: [{ id: 'owner' }] });
        await vi.waitFor(() => expect(bob.state.value).toEqual({ n: 1 }));
        await vi.waitFor(() => expect(carol.closed).toBe(`a read of ${ADDRESS} needs a grant`));
    });
});

/** The owner's mesh server for relays: started once per owner node, with the directory it gives */
const owners = new WeakMap<OwnerNode, string>();
async function startOwnerUrl(owner: OwnerNode, directory?: (url: string) => DirectoryView): Promise<string> {
    const known = owners.get(owner);
    if (known !== undefined) return known;
    const keys = generateKeyPair();
    const local = createRelay(createAuthorityUpstream(async (address) => owner.authorityOf(address), (address) => owner.epochOf(address)), { sweep: false, trustedNodes: () => true });
    let url = '';
    const mesh = await startMeshServer({
        nodeId: 'owner',
        role: 'owner',
        keys,
        certificate: certificateFor('owner', 'owner', keys),
        authorityPublicKey: authority.publicKey,
        upstream: local,
        write: (address, written) => owner.write(address, written),
        directory: () => (directory ?? ((at) => ({ nodes: [{ id: 'owner', role: 'owner' as const, region: '', endpoint: at }], owners: {}, keys: {} })))(url)
    });
    url = mesh.url;
    owners.set(owner, url);
    closers.push(() => mesh.close(), () => local.close());
    return url;
}
