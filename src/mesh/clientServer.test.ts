import { createStore } from '@punyts/state';
import { createStoreBridge, createSyncClient } from '@punyts/state-sync';
import type { Change, SyncTransport } from '@punyts/state-sync';
import { createAuthorityUpstream, createMemoryEpochStorage, createMemoryOwnershipStore, createRelay, generateGrantKey, OwnerNode, signGrant } from '@punyts/state-sync/authority';
import type { Access, Authority, GrantContext } from '@punyts/state-sync/authority';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { namespaceOfAddress, startClientServer } from './ClientServer.js';

const HOUR = 3_600_000;
const ADDRESS = 'alice/notes/user';
const closers: Array<() => Promise<void> | void> = [];

afterEach(async () => {
    for (const close of closers.splice(0).reverse()) await close();
});

/** Alice's owner node, checking grants, with a relay over it and the browsers' server on that relay; `accessOf` names who connects */
async function serve(accessOf: () => Access) {
    const alice = generateGrantKey();
    const context = (): GrantContext => ({ keyOf: (kid) => (kid === 'alice-1' ? { ownerId: 'alice', publicKey: alice.publicKey } : undefined), now: Date.now() });
    const owner = new OwnerNode({ id: 'owner', ownership: createMemoryOwnershipStore(), storage: createMemoryEpochStorage(), link: () => undefined, initial: () => ({ data: {} }), access: () => context() });
    await owner.write(ADDRESS, { id: 'm0', path: '$.data.title', op: 'set', value: 'Mine', base: 0 }, 'owner', { subject: 'alice' });
    const authority = (await owner.authorityOf(ADDRESS)) as Authority;
    const relay = createRelay(createAuthorityUpstream(async (address) => owner.authorityOf(address), (address) => owner.epochOf(address)), { sweep: false });
    const clients = await startClientServer({
        upstream: relay,
        write: (address, change, access) => owner.write(address, change, 'owner', access),
        open: (_address, connection, access) => authority.connect(connection, access),
        accessOf
    });
    closers.push(() => clients.close(), () => owner.stop(), () => relay.close());
    return { alice, authority, clients };
}

/** A browser on protocol 3 over a WebSocket */
function browser(url: string) {
    const transport: SyncTransport = {
        connect: (handlers) => {
            const socket = new WebSocket(url);
            socket.onopen = () => handlers.onOpen();
            socket.onmessage = (event) => handlers.onMessage(String(event.data));
            socket.onclose = () => handlers.onClose();
            return { send: (text) => socket.send(text), close: () => socket.close() };
        }
    };
    const store = createStore({});
    const client = createSyncClient({ namespace: namespaceOfAddress(ADDRESS), bridge: createStoreBridge(store), transport, backoff: () => 20 });
    client.start();
    closers.push(() => client.stop());
    return { store, client };
}

describe("a relay's browsers", () => {
    it("should tell a browser that may only request of its request and of the owner's decision, and give it the requests waiting when it connects again", async () => {
        // Arrange: bob may read and request alice's notes, through the relay
        let grant = '';
        const { alice, authority, clients } = await serve(() => ({ subject: 'bob', grant }));
        grant = signGrant({ kid: 'alice-1', issuer: 'alice', subject: 'bob', owner: 'alice', pattern: '$.notes.user.data', rights: ['read', 'request'], issued: Date.now(), expires: Date.now() + HOUR }, alice.privateKey);
        const bob = browser(clients.url);
        await vi.waitFor(() => expect(bob.store.get('$.data.title')).toBe('Mine'));

        // Act: bob asks; a second browser of bob's connects; alice approves
        bob.store.set('$.data.title', 'Suggested');
        await vi.waitFor(() => expect(bob.client.requests()).toMatchObject([{ path: '$.data.title', value: 'Suggested', mine: true, status: 'pending' }]));
        const again = browser(clients.url);
        await vi.waitFor(() => expect(again.client.requests()).toMatchObject([{ path: '$.data.title', mine: true, status: 'pending' }]));
        const id = bob.client.requests()[0]?.id as string;
        const decided = authority.decide({ id, action: 'approve' }, { subject: 'alice' });

        // Assert
        expect(decided).toEqual({ ok: true, status: 'approved' });
        await vi.waitFor(() => {
            expect(bob.client.requests()).toMatchObject([{ id, status: 'approved' }]);
            expect(bob.store.get('$.data.title')).toBe('Suggested');
        });
        expect(bob.client.status().status).toBe('online');
    });

    it("should pass a browser's decision to the owner's authority", async () => {
        // Arrange: bob asked; alice decides from a browser at the relay
        const { alice, authority, clients } = await serve(() => ({ subject: 'alice' }));
        const grant = signGrant({ kid: 'alice-1', issuer: 'alice', subject: 'bob', owner: 'alice', pattern: '$.notes.user.data', rights: ['request'], issued: Date.now(), expires: Date.now() + HOUR }, alice.privateKey);
        const asked: Change = { id: 'r1', path: '$.data.title', op: 'set', value: 'Suggested', base: 0 };
        const bob = authority.connect({ send: () => undefined, close: () => undefined }, { subject: 'bob', grant });
        bob.receive(JSON.stringify({ type: 'hello', namespace: namespaceOfAddress(ADDRESS), revision: 0, clientId: 'bob-1', protocol: 3 }));
        bob.receive(JSON.stringify({ type: 'change', change: asked }));
        const owner = browser(clients.url);
        await vi.waitFor(() => expect(owner.client.requests()).toMatchObject([{ id: 'r1', mine: false, status: 'pending' }]));

        // Act
        owner.client.reject('r1', 'Keep it');

        // Assert
        await vi.waitFor(() => expect(authority.requests()).toMatchObject([{ status: 'rejected', note: 'Keep it' }]));
        await vi.waitFor(() => expect(owner.client.requests()).toMatchObject([{ id: 'r1', status: 'rejected' }]));
        expect(authority.get('$.data.title')).toBe('Mine');
    });
});
