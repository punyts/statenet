import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { Change } from '@punyts/state-sync';
import { createMemoryEpochStorage, createMemoryOwnershipStore, FencedError, OwnerNode } from '@punyts/state-sync/authority';
import type { EpochStorage, StoredNamespace } from '@punyts/state-sync/authority';
import { addListener, clearListeners, setCategories } from '@punyts/core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createFileEpochStorage } from '../FileEpochStorage.js';
import { createAggregate, createAggregateStorage, startAggregateNode } from './Aggregate.js';
import { generateKeyPair, issueNodeCertificate } from './certificates.js';
import type { KeyPair, NodeRole } from './certificates.js';
import { createMeshUpstream } from './MeshUpstream.js';
import { startRelayNode } from './RelayNode.js';

const HOUR = 3_600_000;
const ADDRESS = 'alice/notes/user';
const authority = generateKeyPair();
const closers: Array<() => Promise<void> | void> = [];

afterEach(async () => {
    for (const close of closers.splice(0).reverse()) await close();
});

const certificateFor = (nodeId: string, role: NodeRole, keys: KeyPair): string => issueNodeCertificate({ nodeId, role, region: '', publicKey: keys.publicKey, expires: Date.now() + HOUR }, authority.privateKey);

const stored = (revision: number, state: StoredNamespace['state'] = { n: revision }): StoredNamespace => ({ state, revision, log: [], applied: [] });

let counter = 0;
const change = (path: string, value: unknown): Change => ({ id: `a${++counter}`, path, op: 'set', value: value as never, base: 0 });

/** Memory storage that counts its saves and can hold them back until `release` */
function gatedStorage() {
    const memory = createMemoryEpochStorage();
    const saved: number[] = [];
    let gate: Promise<void> | undefined;
    let open: (() => void) | undefined;
    const storage: EpochStorage = {
        load: (address) => memory.load(address),
        save: async (address, value, epoch) => {
            await gate;
            await memory.save(address, value, epoch);
            saved.push(value.revision);
        }
    };
    return {
        storage,
        memory,
        saved,
        hold: () => {
            gate = new Promise((resolve) => (open = resolve));
        },
        release: () => {
            gate = undefined;
            open?.();
        }
    };
}

/** An aggregate, a relay linked to it, and a node's link to the relay: the path of a save over real sockets */
async function chain(storage: EpochStorage, mayPersist?: (address: string, node: string | undefined) => boolean) {
    const aggregateKeys = generateKeyPair();
    const aggregate = await startAggregateNode({ nodeId: 'aggregate-1', keys: aggregateKeys, certificate: certificateFor('aggregate-1', 'aggregate', aggregateKeys), authorityPublicKey: authority.publicKey, storage, ...(mayPersist === undefined ? {} : { mayPersist }) });
    const relayKeys = generateKeyPair();
    const relay = await startRelayNode({ nodeId: 'relay-1', keys: relayKeys, certificate: certificateFor('relay-1', 'relay', relayKeys), authorityPublicKey: authority.publicKey, parents: [aggregate.url], checkGrants: false, upstream: { renewWithinMs: 0, followDirectory: false } });
    const nodeKeys = generateKeyPair();
    const link = createMeshUpstream({ nodeId: 'node-1', role: 'owner', keys: nodeKeys, certificate: certificateFor('node-1', 'owner', nodeKeys), authorityPublicKey: authority.publicKey, parents: [relay.meshUrl], followDirectory: false, renewWithinMs: 0 });
    closers.push(() => link.close(), () => relay.close(), () => aggregate.close());
    await vi.waitFor(() => expect(link.linked() && relay.upstream.linked()).toBe(true));
    return { aggregate, relay, link };
}

describe('the aggregate', () => {
    it('should save a namespace and answer with the revision that landed', async () => {
        // Arrange
        const { storage, memory, saved } = gatedStorage();
        const aggregate = createAggregate(storage);

        // Act
        const outcome = await aggregate.persist(ADDRESS, stored(3), 1);

        // Assert
        expect(outcome).toEqual({ result: 'saved', revision: 3 });
        expect(saved).toEqual([3]);
        expect((await memory.load(ADDRESS))?.state).toEqual({ n: 3 });
    });

    it('should write nothing for a save sent again, or one below what already landed', async () => {
        // Arrange
        const { storage, saved } = gatedStorage();
        const aggregate = createAggregate(storage);
        await aggregate.persist(ADDRESS, stored(5), 1);

        // Act
        const again = await aggregate.persist(ADDRESS, stored(5), 1);
        const older = await aggregate.persist(ADDRESS, stored(4), 1);

        // Assert
        expect(again).toEqual({ result: 'saved', revision: 5 });
        expect(older).toEqual({ result: 'saved', revision: 5 });
        expect(saved).toEqual([5]);
        expect(aggregate.stats()).toMatchObject({ saves: 1, folded: 2 });
    });

    it('should write nothing for a save sent again after the aggregate restarted, nor take the state back, in memory or in files', async () => {
        // Arrange: a save landed through one aggregate, which then stopped; another starts over the same storage
        const dataDir = await mkdtemp(join(tmpdir(), 'statenet-aggregate-'));
        closers.push(() => rm(dataDir, { recursive: true, force: true }));
        const storages = [createMemoryEpochStorage(), createFileEpochStorage({ dataDir })];
        for (const storage of storages) await createAggregate(storage).persist(ADDRESS, stored(5, { n: 'five' }), 2);

        // Act
        const answers = [];
        for (const storage of storages) {
            const restarted = createAggregate(storage);
            answers.push(await restarted.persist(ADDRESS, stored(5, { n: 'sent again' }), 2), await restarted.persist(ADDRESS, stored(4, { n: 'older' }), 2));
        }
        const states = await Promise.all(storages.map(async (storage) => (await storage.load(ADDRESS))?.state));
        const next = await createAggregate(storages[1] as EpochStorage).persist(ADDRESS, stored(6, { n: 'six' }), 2);

        // Assert
        expect(answers.map((answer) => answer.result)).toEqual(['saved', 'saved', 'saved', 'saved']);
        expect(states).toEqual([{ n: 'five' }, { n: 'five' }]);
        expect(next).toEqual({ result: 'saved', revision: 6 });
        expect((await (storages[1] as EpochStorage).load(ADDRESS))?.state).toEqual({ n: 'six' });
    });

    it('should write a save that takes no revision ($.server, a request) by its serial, through the aggregate, in memory and in files', async () => {
        // Arrange
        const dataDir = await mkdtemp(join(tmpdir(), 'statenet-aggregate-'));
        closers.push(() => rm(dataDir, { recursive: true, force: true }));
        const storages = [createMemoryEpochStorage(), createFileEpochStorage({ dataDir })];
        const serialed = (serial: number, n: string): StoredNamespace => ({ ...stored(5, { n }), serial });

        // Act
        for (const storage of storages) {
            const aggregate = createAggregate(storage);
            await aggregate.persist(ADDRESS, serialed(5, 'five'), 2);
            await aggregate.persist(ADDRESS, serialed(6, 'a request'), 2);
            await aggregate.persist(ADDRESS, serialed(6, 'sent again'), 2);
            await createAggregate(storage).persist(ADDRESS, serialed(6, 'after a restart'), 2);
        }

        // Assert
        const states = await Promise.all(storages.map(async (storage) => (await storage.load(ADDRESS))?.state));
        expect(states).toEqual([{ n: 'a request' }, { n: 'a request' }]);
    });

    it('should fold the saves that arrive while one runs into one batch, and answer each when it lands', async () => {
        // Arrange
        const { storage, saved, hold, release } = gatedStorage();
        const aggregate = createAggregate(storage);
        hold();

        // Act
        const first = aggregate.persist(ADDRESS, stored(1), 1);
        const second = aggregate.persist(ADDRESS, stored(2), 1);
        const third = aggregate.persist(ADDRESS, stored(3), 1);
        let answered = false;
        void third.then(() => (answered = true));
        await new Promise((resolve) => setTimeout(resolve, 20));
        const before = answered;
        release();

        // Assert
        expect(before).toBe(false);
        expect(await Promise.all([first, second, third])).toEqual([
            { result: 'saved', revision: 1 },
            { result: 'saved', revision: 3 },
            { result: 'saved', revision: 3 }
        ]);
        expect(saved).toEqual([1, 3]);
    });

    it('should fence a save under an older epoch and write nothing, by its own count and by the storage\'s', async () => {
        // Arrange: the storage already holds epoch 4, which a new aggregate has not seen
        const { storage, memory, saved } = gatedStorage();
        await memory.save(ADDRESS, stored(9), 4);
        const aggregate = createAggregate(storage);

        // Act
        const byStorage = await aggregate.persist(ADDRESS, stored(10), 3);
        const byCount = await aggregate.persist(ADDRESS, stored(11), 3);
        const newer = await aggregate.persist(ADDRESS, stored(12), 5);

        // Assert
        expect(byStorage).toEqual({ result: 'fenced', epoch: 3, current: 4 });
        expect(byCount).toEqual({ result: 'fenced', epoch: 3, current: 4 });
        expect(newer).toEqual({ result: 'saved', revision: 12 });
        expect(saved).toEqual([12]);
        expect(aggregate.stats().fenced).toBe(2);
    });

    it('should answer a waiting save of the old owner as fenced when the new owner\'s save takes its place', async () => {
        // Arrange
        const { storage, saved, hold, release } = gatedStorage();
        const aggregate = createAggregate(storage);
        hold();
        const running = aggregate.persist(ADDRESS, stored(1), 1);

        // Act
        const old = aggregate.persist(ADDRESS, stored(2), 1);
        const taken = aggregate.persist(ADDRESS, stored(2, { by: 'new' }), 2);
        release();

        // Assert
        expect(await old).toEqual({ result: 'fenced', epoch: 1, current: 2 });
        expect(await running).toEqual({ result: 'saved', revision: 1 });
        expect(await taken).toEqual({ result: 'saved', revision: 2 });
        expect(saved).toEqual([1, 2]);
    });

    it('should refuse a save from a node that does not order the namespace, and say a failed save failed', async () => {
        // Arrange
        const broken: EpochStorage = { load: async () => undefined, save: async () => Promise.reject(new Error('the disk is full')) };
        const guarded = createAggregate(createMemoryEpochStorage(), { mayPersist: (_address, node) => node === 'node-1' });
        const failing = createAggregate(broken);

        // Act
        const outcomes = [await guarded.persist(ADDRESS, stored(1), 1, 'node-2'), await guarded.persist(ADDRESS, stored(1), 1, 'node-1'), await failing.persist(ADDRESS, stored(1), 1)];

        // Assert
        expect(outcomes).toEqual([{ result: 'failed', reason: `node-2 does not order ${ADDRESS}` }, { result: 'saved', revision: 1 }, { result: 'failed', reason: 'the disk is full' }]);
    });
});

describe('removing a namespace', () => {
    it('should remove it after the saves that came before, refuse an older epoch, and take a save again after', async () => {
        // Arrange
        const dataDir = await mkdtemp(join(tmpdir(), 'statenet-remove-'));
        closers.push(() => rm(dataDir, { recursive: true, force: true }));
        const files = createFileEpochStorage({ dataDir });
        const aggregate = createAggregate(files);

        // Act
        const saving = aggregate.persist(ADDRESS, stored(4), 2);
        const older = await aggregate.remove(ADDRESS, 1);
        const removed = await aggregate.remove(ADDRESS, 2);
        const gone = await files.load(ADDRESS);
        const late = await aggregate.persist(ADDRESS, stored(5), 1);
        const again = await aggregate.persist(ADDRESS, stored(1), 2);

        // Assert
        expect(await saving).toEqual({ result: 'saved', revision: 4 });
        expect(older).toEqual({ result: 'fenced', epoch: 1, current: 2 });
        expect(removed).toEqual({ result: 'removed' });
        expect(gone).toBeUndefined();
        expect(late).toEqual({ result: 'fenced', epoch: 1, current: 2 });
        expect(again).toEqual({ result: 'saved', revision: 1 });
        expect((await files.load(ADDRESS))?.revision).toBe(1);
    });

    it('should say so when its storage removes nothing, and remove over a relay for the node that asks', async () => {
        // Arrange
        const memory = createMemoryEpochStorage();
        const removed: string[] = [];
        const { link } = await chain({ load: (address) => memory.load(address), save: (address, value, epoch) => memory.save(address, value, epoch), remove: async (address: string) => void removed.push(address) } as EpochStorage);
        const plain = createAggregate(memory);
        await createAggregateStorage(link).save(ADDRESS, stored(1), 1);

        // Act
        const unable = await plain.remove(ADDRESS, 1);
        await createAggregateStorage(link).remove(ADDRESS, 1);

        // Assert
        expect(unable).toEqual({ result: 'failed', reason: "this aggregate's storage removes nothing" });
        expect(removed).toEqual([ADDRESS]);
    });
});

describe('a node that saves through an aggregate', () => {
    it('should call a change durable only once the aggregate saved it', async () => {
        // Arrange: a node in the same process as its aggregate
        const { storage, memory, hold, release } = gatedStorage();
        const aggregate = createAggregate(storage);
        const node = new OwnerNode({ id: 'node-1', ownership: createMemoryOwnershipStore(), storage: createAggregateStorage(aggregate, 'node-1'), link: () => undefined, initial: () => ({ data: {} }) });
        closers.push(() => node.stop());
        hold();

        // Act
        const written = await node.write(ADDRESS, change('$.data.n', 1));
        const authorityOf = await node.authorityOf(ADDRESS);
        let durable = false;
        void authorityOf?.whenDurable().then(() => (durable = true));
        await new Promise((resolve) => setTimeout(resolve, 30));
        const before = durable;
        release();

        // Assert
        expect(written).toMatchObject({ result: 'applied' });
        expect(before).toBe(false);
        await vi.waitFor(() => expect(durable).toBe(true));
        expect((await memory.load(ADDRESS))?.state).toEqual({ data: { n: 1 } });
    });

    it('should throw FencedError from a save the aggregate fenced, and the reason of one that failed', async () => {
        // Arrange
        const memory = createMemoryEpochStorage();
        await memory.save(ADDRESS, stored(1), 2);
        const storage = createAggregateStorage(createAggregate(memory, { mayPersist: (_address, _node, epoch) => epoch !== 7 }), 'node-1');

        // Act
        const fenced = await storage.save(ADDRESS, stored(2), 1).catch((error: unknown) => error);
        const failed = await storage.save(ADDRESS, stored(2), 7).catch((error: unknown) => error);

        // Assert
        expect(fenced).toBeInstanceOf(FencedError);
        expect(fenced).toMatchObject({ epoch: 1, current: 2 });
        expect((failed as Error).message).toContain('does not order');
    });

    it('should save and load over its link to a relay and the relay\'s to an aggregate, the aggregate hearing which node it is', async () => {
        // Arrange
        const memory = createMemoryEpochStorage();
        const heard: Array<string | undefined> = [];
        const { link, aggregate } = await chain(memory, (_address, node) => {
            heard.push(node);
            return true;
        });
        const node = new OwnerNode({ id: 'node-1', ownership: createMemoryOwnershipStore(), storage: createAggregateStorage(link), link: () => undefined, initial: () => ({ data: {} }) });
        closers.push(() => node.stop());

        // Act
        await node.write(ADDRESS, change('$.data.n', 1));
        await node.write(ADDRESS, change('$.data.m', 2));
        await (await node.authorityOf(ADDRESS))?.whenDurable();
        const loaded = await createAggregateStorage(link).load(ADDRESS);
        const nothing = await createAggregateStorage(link).load('alice/other/user');

        // Assert
        expect((await memory.load(ADDRESS))?.state).toEqual({ data: { n: 1, m: 2 } });
        expect(loaded?.state).toEqual({ data: { n: 1, m: 2 } });
        expect(nothing).toBeUndefined();
        expect(new Set(heard)).toEqual(new Set(['node-1']));
        expect(aggregate.aggregate.stats().saves).toBeGreaterThan(0);
    });

    it('should send a save again over another relay when its link drops, the aggregate writing it once', async () => {
        // Arrange: two relays under one aggregate, the node linked to the first, and a save held at the storage
        const { storage, saved, hold, release } = gatedStorage();
        const { aggregate, relay, link } = await chain(storage);
        const otherKeys = generateKeyPair();
        const other = await startRelayNode({ nodeId: 'relay-2', keys: otherKeys, certificate: certificateFor('relay-2', 'relay', otherKeys), authorityPublicKey: authority.publicKey, parents: [aggregate.url], checkGrants: false, upstream: { renewWithinMs: 0, followDirectory: false } });
        const nodeKeys = generateKeyPair();
        const node = createMeshUpstream({ nodeId: 'node-2', role: 'owner', keys: nodeKeys, certificate: certificateFor('node-2', 'owner', nodeKeys), authorityPublicKey: authority.publicKey, parents: [relay.meshUrl, other.meshUrl], followDirectory: false, renewWithinMs: 0, backoff: () => 20 });
        closers.push(() => node.close(), () => other.close());
        await vi.waitFor(() => expect(node.linked() && other.upstream.linked()).toBe(true));
        link.close();
        hold();
        const pending = node.persist(ADDRESS, stored(1), 1);
        await vi.waitFor(() => expect(aggregate.mesh.stats().links).toHaveLength(2));
        await new Promise((resolve) => setTimeout(resolve, 30));

        // Act: the first relay goes away with the save unanswered
        await relay.close();
        await vi.waitFor(() => expect(node.parent()).toBe(other.meshUrl));
        await new Promise((resolve) => setTimeout(resolve, 30));
        release();

        // Assert
        expect(await pending).toEqual({ result: 'saved', revision: 1 });
        expect(saved).toEqual([1]);
        expect(aggregate.aggregate.stats().folded).toBe(1);
    });

    it('should answer a save still waiting when its link is closed', async () => {
        // Arrange: a relay with nothing above it, so what it passes up is never answered
        const topKeys = generateKeyPair();
        const top = await startRelayNode({ nodeId: 'relay-top', keys: topKeys, certificate: certificateFor('relay-top', 'relay', topKeys), authorityPublicKey: authority.publicKey, parents: [], checkGrants: false });
        const nodeKeys = generateKeyPair();
        const link = createMeshUpstream({ nodeId: 'node-1', role: 'owner', keys: nodeKeys, certificate: certificateFor('node-1', 'owner', nodeKeys), authorityPublicKey: authority.publicKey, parents: [top.meshUrl], followDirectory: false, renewWithinMs: 0 });
        closers.push(() => link.close(), () => top.close());
        await vi.waitFor(() => expect(link.linked()).toBe(true));

        // Act
        const pending = link.persist(ADDRESS, stored(1), 1);
        link.close();

        // Assert
        expect(await pending).toEqual({ result: 'failed', reason: 'the link is closed' });
    });

    it('should report the links, the relay and the save by node, address and revision, and never the values', async () => {
        // Arrange
        const heard: Array<{ category: string; message: string }> = [];
        setCategories(['statenet.*']);
        addListener((_timestamp, category, message) => heard.push({ category, message }), ['statenet.*']);
        try {
            const { link } = await chain(createMemoryEpochStorage());

            // Act
            const outcome = await link.persist(ADDRESS, stored(1, { card: 'card-4242-secret' }), 1);

            // Assert
            const messages = (category: string) => heard.filter((entry) => entry.category === category).map((entry) => entry.message);
            expect(outcome).toEqual({ result: 'saved', revision: 1 });
            expect(messages('statenet.mesh').some((message) => message.startsWith('Node relay-1 opened a link from node-1 (owner)'))).toBe(true);
            expect(messages('statenet.mesh').some((message) => message.startsWith('Node node-1 linked to relay-1 at ws://'))).toBe(true);
            expect(messages('statenet.relay').some((message) => message.startsWith('Relay relay-1 started: links at ws://'))).toBe(true);
            expect(messages('statenet.mesh.serving')).toContain(`Node aggregate-1 saved ${ADDRESS} for node-1 at revision 1: saved`);
            expect(messages('statenet.aggregate').some((message) => /^Saved alice\/notes\/user at revision 1 under epoch 1 in [\d.]+ ms, for 1 waiting$/.test(message))).toBe(true);
            expect(JSON.stringify(heard)).not.toContain('card-4242-secret');
        } finally {
            setCategories(['info', 'error', 'stack']);
            clearListeners();
        }
    });
});

describe('epoch storage in files', () => {
    it('should keep a namespace and its epoch across starts, and refuse a save under an older epoch', async () => {
        // Arrange
        const dataDir = await mkdtemp(join(tmpdir(), 'statenet-aggregate-'));
        closers.push(() => rm(dataDir, { recursive: true, force: true }));
        const first = createFileEpochStorage({ dataDir });
        await first.save(ADDRESS, stored(1), 1);
        await first.save(ADDRESS, stored(2), 3);

        // Act
        const second = createFileEpochStorage({ dataDir });
        const refused = await second.save(ADDRESS, stored(3), 2).catch((error: unknown) => error);
        await second.save(ADDRESS, stored(4), 3);

        // Assert
        expect(refused).toBeInstanceOf(FencedError);
        expect(refused).toMatchObject({ epoch: 2, current: 3 });
        expect((await second.load(ADDRESS))?.revision).toBe(4);
        expect(await second.load('alice/other/user')).toBeUndefined();
    });
});

describe('an aggregate that lists its relays', () => {
    it('should list a relay from its certificate when its link opens, and take it out when the link closes', async () => {
        // Arrange
        const listed: unknown[] = [];
        const unlisted: string[] = [];
        const aggregateKeys = generateKeyPair();
        const aggregate = await startAggregateNode({
            nodeId: 'aggregate-1',
            keys: aggregateKeys,
            certificate: certificateFor('aggregate-1', 'aggregate', aggregateKeys),
            authorityPublicKey: authority.publicKey,
            storage: createMemoryEpochStorage(),
            relays: {
                list: async (relay) => void listed.push(relay),
                unlist: async (nodeId) => void unlisted.push(nodeId)
            }
        });
        closers.push(() => aggregate.close());
        const relayKeys = generateKeyPair();
        const certificate = issueNodeCertificate({ nodeId: 'relay-1', role: 'relay', region: 'east', group: 'shop', endpoint: 'wss://relay-1.example:4310', publicKey: relayKeys.publicKey, expires: Date.now() + HOUR }, authority.privateKey);

        // Act
        const relay = await startRelayNode({ nodeId: 'relay-1', region: 'east', keys: relayKeys, certificate, authorityPublicKey: authority.publicKey, parents: [aggregate.url], checkGrants: false, upstream: { renewWithinMs: 0, followDirectory: false } });
        await vi.waitFor(() => expect(listed).toHaveLength(1));
        const whileLinked = aggregate.relays().map((each) => each.id);
        await relay.close();
        await vi.waitFor(() => expect(unlisted).toEqual(['relay-1']));

        // Assert
        expect(listed).toEqual([{ id: 'relay-1', region: 'east', group: 'shop', endpoint: 'wss://relay-1.example:4310', publicKey: relayKeys.publicKey, parent: 'aggregate-1' }]);
        expect(whileLinked).toEqual(['relay-1']);
        expect(aggregate.relays()).toEqual([]);
    });

    it('should list a relay whose certificate names no group or endpoint with empty ones', async () => {
        // Arrange
        const listed: unknown[] = [];
        const { storage } = gatedStorage();
        const aggregateKeys = generateKeyPair();
        const aggregate = await startAggregateNode({
            nodeId: 'aggregate-1',
            keys: aggregateKeys,
            certificate: certificateFor('aggregate-1', 'aggregate', aggregateKeys),
            authorityPublicKey: authority.publicKey,
            storage,
            relays: { list: async (relay) => void listed.push(relay), unlist: async () => undefined }
        });
        closers.push(() => aggregate.close());
        const relayKeys = generateKeyPair();

        // Act
        const relay = await startRelayNode({ nodeId: 'relay-1', keys: relayKeys, certificate: certificateFor('relay-1', 'relay', relayKeys), authorityPublicKey: authority.publicKey, parents: [aggregate.url], checkGrants: false, upstream: { renewWithinMs: 0, followDirectory: false } });
        closers.push(() => relay.close());
        await vi.waitFor(() => expect(relay.upstream.linked()).toBe(true));
        await vi.waitFor(() => expect(listed).toHaveLength(1));

        // Assert
        expect(listed).toEqual([expect.objectContaining({ id: 'relay-1', group: '', endpoint: '' })]);
    });
});
