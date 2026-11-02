import { randomUUID } from "node:crypto";
import { mkdir, readdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { NamespaceStorage, StoredNamespace } from "@punyts/state-sync/authority";

/** The file of a namespace inside its folder */
export const NAMESPACE_FILE = "namespace.json";

const TEMPORARY_SUFFIX = ".tmp";
const RETRYABLE = new Set(["EPERM", "EBUSY", "EACCES"]);
const RENAME_ATTEMPTS = 5;
/** The namespace pattern of the sync protocol: it keeps a namespace a safe folder name (no separators, no "..") */
const NAMESPACE = /^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$/;

const errorCode = (error: unknown): string | undefined =>
    typeof error === "object" && error !== null && "code" in error && typeof error.code === "string" ? error.code : undefined;

const isRecord = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);

const isStoredNamespace = (value: unknown): value is StoredNamespace =>
    isRecord(value) &&
    isRecord(value["state"]) &&
    typeof value["revision"] === "number" &&
    Array.isArray(value["log"]) &&
    Array.isArray(value["applied"]);

const renameWithRetry = async (from: string, to: string): Promise<void> => {
    for (let attempt = 1; ; attempt++) {
        try {
            await rename(from, to);
            return;
        } catch (error) {
            const code = errorCode(error);
            // Windows refuses a rename onto a file that a reader (or a virus scanner) has open for a moment
            if (attempt >= RENAME_ATTEMPTS || code === undefined || !RETRYABLE.has(code)) throw error;
            await new Promise((resolve) => setTimeout(resolve, 10 * attempt));
        }
    }
};

export interface FileNamespaceStorageOptions {
    /** The folder that holds one folder per namespace */
    dataDir: string;
    /** Writes the bytes of a file; for tests that make a write fail. `writeFile` by default */
    writeFile?: (path: string, data: string) => Promise<void>;
}

/**
 * Namespace storage as JSON files: one folder per namespace (named by the namespace) with `namespace.json`, the state,
 * revision, change log and applied change ids. A save writes a temporary file and renames it over the last one, so a
 * crash leaves the last good file; the saves of one namespace run one at a time, in order. Temporary files a crash
 * left behind are removed when the namespace loads
 */
export const createFileNamespaceStorage = (options: FileNamespaceStorageOptions): NamespaceStorage => {
    const write = options.writeFile ?? ((path: string, data: string) => writeFile(path, data, "utf8"));
    const queues = new Map<string, Promise<void>>();

    const folderOf = (namespace: string): string => {
        if (!NAMESPACE.test(namespace)) throw new Error(`${namespace} is not a namespace`);
        return join(options.dataDir, namespace);
    };

    const removeTemporaries = async (folder: string): Promise<void> => {
        let names: string[];
        try {
            names = await readdir(folder);
        } catch (error) {
            if (errorCode(error) === "ENOENT") return;
            throw error;
        }
        await Promise.all(names.filter((name) => name.endsWith(TEMPORARY_SUFFIX)).map((name) => rm(join(folder, name), { force: true })));
    };

    const saveNow = async (namespace: string, stored: StoredNamespace): Promise<void> => {
        const folder = folderOf(namespace);
        await mkdir(folder, { recursive: true });
        const file = join(folder, NAMESPACE_FILE);
        const temporary = `${file}.${randomUUID()}${TEMPORARY_SUFFIX}`;
        try {
            await write(temporary, JSON.stringify(stored));
            await renameWithRetry(temporary, file);
        } catch (error) {
            await rm(temporary, { force: true });
            throw error;
        }
    };

    return {
        load: async (namespace) => {
            const folder = folderOf(namespace);
            await queues.get(namespace);
            await removeTemporaries(folder);
            let text: string;
            try {
                text = await readFile(join(folder, NAMESPACE_FILE), "utf8");
            } catch (error) {
                if (errorCode(error) === "ENOENT") return undefined;
                throw error;
            }
            const parsed: unknown = JSON.parse(text);
            if (!isStoredNamespace(parsed)) throw new Error(`The stored namespace ${namespace} is damaged`);
            return parsed;
        },
        save: (namespace, stored) => {
            const previous = queues.get(namespace) ?? Promise.resolve();
            const next = previous.catch(() => undefined).then(() => saveNow(namespace, stored));
            const settled = next.catch(() => undefined);
            queues.set(namespace, settled);
            void settled.then(() => {
                if (queues.get(namespace) === settled) queues.delete(namespace);
            });
            return next;
        }
    };
};
