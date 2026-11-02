import { mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { StoredNamespace } from "@punyts/state-sync/authority";
import { createFileNamespaceStorage, NAMESPACE_FILE } from "./FileNamespaceStorage.js";

const stored = (revision: number, total: number): StoredNamespace =>
    Object.freeze({ state: { data: { total } }, revision, log: [], applied: [] }) as StoredNamespace;

describe("createFileNamespaceStorage", () => {
    let dataDir = "";

    beforeEach(async () => {
        dataDir = await mkdtemp(join(tmpdir(), "statenet-storage-"));
    });

    afterEach(async () => {
        await rm(dataDir, { recursive: true, force: true });
    });

    it("should load nothing for a namespace that was never saved", async () => {
        const storage = createFileNamespaceStorage({ dataDir });

        expect(await storage.load("apps.none")).toBeUndefined();
    });

    it("should keep one folder per namespace and load what was saved last", async () => {
        // Arrange
        const storage = createFileNamespaceStorage({ dataDir });

        // Act: saves of one namespace are not awaited one by one, so they must still land in order
        await Promise.all([storage.save("apps.a", stored(1, 1)), storage.save("apps.a", stored(2, 2)), storage.save("apps.b", stored(5, 5))]);

        // Assert
        expect((await storage.load("apps.a"))?.revision).toBe(2);
        expect((await storage.load("apps.b"))?.state).toEqual({ data: { total: 5 } });
        expect((await readdir(dataDir)).sort()).toEqual(["apps.a", "apps.b"]);
        expect(await readdir(join(dataDir, "apps.a"))).toEqual([NAMESPACE_FILE]);
    });

    it("should leave the last good file when a write crashes, and remove the temporary file on the next load", async () => {
        // Arrange: the second write dies after half its bytes, as a crash would leave it
        let calls = 0;
        const storage = createFileNamespaceStorage({
            dataDir,
            writeFile: async (path, data) => {
                calls++;
                if (calls === 2) {
                    await writeFile(path, data.slice(0, data.length / 2));
                    throw new Error("crash");
                }
                await writeFile(path, data);
            }
        });
        await storage.save("apps.a", stored(1, 1));

        // Act
        await expect(storage.save("apps.a", stored(2, 2))).rejects.toThrow("crash");
        await writeFile(join(dataDir, "apps.a", `${NAMESPACE_FILE}.left-by-a-crash.tmp`), "{");
        const loaded = await storage.load("apps.a");

        // Assert
        expect(loaded?.revision).toBe(1);
        expect(await readdir(join(dataDir, "apps.a"))).toEqual([NAMESPACE_FILE]);
    });

    it("should refuse a damaged file and a name that is not a namespace", async () => {
        const storage = createFileNamespaceStorage({ dataDir });
        await storage.save("apps.a", stored(1, 1));
        await writeFile(join(dataDir, "apps.a", NAMESPACE_FILE), JSON.stringify({ state: 1 }));

        await expect(storage.load("apps.a")).rejects.toThrow("damaged");
        await expect(storage.load("../escape")).rejects.toThrow("not a namespace");
        await expect(storage.save("a/b", stored(1, 1))).rejects.toThrow("not a namespace");
    });
});
