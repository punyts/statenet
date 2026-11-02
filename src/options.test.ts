import { resolve } from "node:path";
import { isLoopbackHost, hostNameOf } from "./loopback.js";
import { parseOptions, UsageError } from "./options.js";

describe("parseOptions", () => {
    it("should use the defaults, then the environment, then the flags", () => {
        const defaults = parseOptions([], {});
        const fromEnv = parseOptions([], { STATENET_PORT: "4300", STATENET_LOG_LENGTH: "10", STATENET_ALLOW_ORIGINS: "http://127.0.0.1:4100, http://localhost:4100" });
        const fromFlags = parseOptions(
            ["--port", "4400", "--host=::1", "--data-dir", "store", "--log-length", "5", "--allow-origin", "http://127.0.0.1:5000"],
            { STATENET_PORT: "4300", STATENET_ALLOW_ORIGINS: "http://localhost:4100" }
        );

        expect(defaults).toEqual({ dataDir: resolve("./data"), host: "127.0.0.1", port: 4200, logLength: 1000, allowedOrigins: [], help: false });
        expect(fromEnv).toMatchObject({ port: 4300, logLength: 10, allowedOrigins: ["http://127.0.0.1:4100", "http://localhost:4100"] });
        expect(fromFlags).toEqual({ dataDir: resolve("store"), host: "::1", port: 4400, logLength: 5, allowedOrigins: ["http://127.0.0.1:5000"], help: false });
    });

    it("should refuse a bad port, log length, origin or option", () => {
        expect(() => parseOptions(["--port", "70000"], {})).toThrow(UsageError);
        expect(() => parseOptions(["--log-length", "-1"], {})).toThrow(UsageError);
        expect(() => parseOptions(["--allow-origin", "http://x.test/path"], {})).toThrow(UsageError);
        expect(() => parseOptions(["--nope"], {})).toThrow(UsageError);
        expect(() => parseOptions(["--port"], {})).toThrow(UsageError);
    });
});

describe("isLoopbackHost", () => {
    it("should accept loopback addresses and names only", () => {
        expect(["127.0.0.1", "127.1.2.3", "localhost", "LOCALHOST", "::1", "[::1]"].map(isLoopbackHost)).toEqual([true, true, true, true, true, true]);
        expect(["0.0.0.0", "192.168.1.10", "127.0.0.256", "example.com", "::"].map(isLoopbackHost)).toEqual([false, false, false, false, false]);
    });

    it("should read the host name of a Host header", () => {
        expect([hostNameOf("127.0.0.1:4200"), hostNameOf("[::1]:4200"), hostNameOf("localhost"), hostNameOf(undefined)]).toEqual(["127.0.0.1", "[::1]", "localhost", undefined]);
    });
});
