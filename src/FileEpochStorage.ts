import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { FencedError } from "@punyts/state-sync/authority";
import type { EpochStorage } from "@punyts/state-sync/authority";
import { createFileNamespaceStorage, NAMESPACE_FILE } from "./FileNamespaceStorage.js";
import type { FileNamespaceStorageOptions } from "./FileNamespaceStorage.js";

/** The file beside a namespace's that keeps the newest epoch it was saved under and the revision saved under it */
export const EPOCH_FILE = "epoch.json";

/**
 * Epoch storage as files, an aggregate's storage where there is no database (docs/design/statenet-network.md,
 * "Locally"): each namespace as `createFileNamespaceStorage` keeps it, in a folder named by its address with dots for
 * slashes, and beside it the newest epoch it was saved under with the revision that landed. A save under an older epoch
 * is refused with `FencedError`; one at or below the revision that landed under its own epoch writes nothing. The saves of one address run one at a time
 */
export const createFileEpochStorage = (options: FileNamespaceStorageOptions): EpochStorage & { remove(address: string, epoch?: number): Promise<void> } => {
    const files = createFileNamespaceStorage(options);
    const queues = new Map<string, Promise<void>>();
    const nameOf = (address: string): string => address.split("/").join(".");
    const epochFile = (address: string): string => join(options.dataDir, nameOf(address), EPOCH_FILE);

    const headOf = async (address: string): Promise<{ epoch: number; revision: number }> => {
        try {
            const parsed = JSON.parse(await readFile(epochFile(address), "utf8")) as { epoch?: unknown; revision?: unknown };
            return { epoch: typeof parsed.epoch === "number" ? parsed.epoch : 0, revision: typeof parsed.revision === "number" ? parsed.revision : 0 };
        } catch {
            return { epoch: 0, revision: 0 };
        }
    };
    const writeHead = async (address: string, head: { epoch: number; revision: number }): Promise<void> => {
        await mkdir(join(options.dataDir, nameOf(address)), { recursive: true });
        await writeFile(epochFile(address), JSON.stringify(head), "utf8");
    };

    return {
        load: (address) => files.load(nameOf(address)),
        // the namespace's file goes, after the saves that came before; its epoch stays, so an older owner stays refused,
        // and what landed is forgotten, so the next save writes
        remove: (address, epoch) => {
            const turn = (queues.get(address) ?? Promise.resolve()).then(async () => {
                const current = (await headOf(address)).epoch;
                if (epoch !== undefined && epoch < current) throw new FencedError(address, epoch, current);
                await rm(join(options.dataDir, nameOf(address), NAMESPACE_FILE), { force: true });
                if (current > 0) await writeHead(address, { epoch: current, revision: 0 });
            });
            queues.set(address, turn.catch(() => undefined));
            return turn;
        },
        save: (address, stored, epoch) => {
            const turn = (queues.get(address) ?? Promise.resolve()).then(async () => {
                const head = await headOf(address);
                if (epoch < head.epoch) throw new FencedError(address, epoch, head.epoch);
                // a save sent again (after the aggregate restarted, or over another relay) is what landed
                if (epoch === head.epoch && head.revision > 0 && stored.revision <= head.revision) return;
                await files.save(nameOf(address), stored);
                await writeHead(address, { epoch, revision: stored.revision });
            });
            queues.set(address, turn.catch(() => undefined));
            return turn;
        }
    };
};
