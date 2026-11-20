import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { FencedError } from "@punyts/state-sync/authority";
import type { EpochStorage } from "@punyts/state-sync/authority";
import { createFileNamespaceStorage } from "./FileNamespaceStorage.js";
import type { FileNamespaceStorageOptions } from "./FileNamespaceStorage.js";

/** The file beside a namespace's that keeps the newest epoch it was saved under */
export const EPOCH_FILE = "epoch.json";

/**
 * Epoch storage as files, an aggregate's storage where there is no database (docs/design/statenet-network.md,
 * "Locally"): each namespace as `createFileNamespaceStorage` keeps it, in a folder named by its address with dots for
 * slashes, and beside it the newest epoch it was saved under. A save under an older epoch is refused with
 * `FencedError`. The saves of one address run one at a time
 */
export const createFileEpochStorage = (options: FileNamespaceStorageOptions): EpochStorage => {
    const files = createFileNamespaceStorage(options);
    const queues = new Map<string, Promise<void>>();
    const nameOf = (address: string): string => address.split("/").join(".");
    const epochFile = (address: string): string => join(options.dataDir, nameOf(address), EPOCH_FILE);

    const epochOf = async (address: string): Promise<number> => {
        try {
            const parsed = JSON.parse(await readFile(epochFile(address), "utf8")) as { epoch?: unknown };
            return typeof parsed.epoch === "number" ? parsed.epoch : 0;
        } catch {
            return 0;
        }
    };

    return {
        load: (address) => files.load(nameOf(address)),
        save: (address, stored, epoch) => {
            const turn = (queues.get(address) ?? Promise.resolve()).then(async () => {
                const current = await epochOf(address);
                if (epoch < current) throw new FencedError(address, epoch, current);
                if (epoch > current) {
                    await mkdir(join(options.dataDir, nameOf(address)), { recursive: true });
                    await writeFile(epochFile(address), JSON.stringify({ epoch }), "utf8");
                }
                await files.save(nameOf(address), stored);
            });
            queues.set(address, turn.catch(() => undefined));
            return turn;
        }
    };
};
