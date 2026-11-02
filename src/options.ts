import { resolve } from "node:path";
import { DEFAULT_LOG_LENGTH, DEFAULT_PORT } from "./Statenet.js";

export interface CliOptions {
    dataDir: string;
    host: string;
    port: number;
    logLength: number;
    allowedOrigins: string[];
    help: boolean;
}

export const HELP_TEXT = `statenet: persists @punyts/state-sync namespaces and speaks the sync protocol at ws://<host>:<port>/sync

Usage: statenet [options]

Options:
  --data-dir <dir>       Where the namespaces are stored, one folder each
                         (env STATENET_DATA_DIR, default ./data)
  --port <number>        Port to listen on (env STATENET_PORT, default ${DEFAULT_PORT})
  --host <address>       Loopback address to bind (env STATENET_HOST, default 127.0.0.1).
                         Any other address is refused: the statenet has no authentication
  --log-length <n>       Recent changes each namespace keeps for catching clients up
                         (env STATENET_LOG_LENGTH, default ${DEFAULT_LOG_LENGTH})
  --allow-origin <url>   A browser origin that may connect, such as http://127.0.0.1:4100;
                         repeat for more (env STATENET_ALLOW_ORIGINS, comma-separated).
                         Clients that send no Origin (Node, app servers) are always accepted
  -h, --help             Show this help
`;

export class UsageError extends Error {}

const parseInteger = (text: string, what: string, min: number, max: number): number => {
    const value = Number(text);
    if (!Number.isInteger(value) || value < min || value > max) throw new UsageError(`Invalid ${what}: ${text}`);
    return value;
};

const parseOrigin = (text: string): string => {
    let url: URL;
    try {
        url = new URL(text);
    } catch {
        throw new UsageError(`Invalid origin: ${text}`);
    }
    if ((url.protocol !== "http:" && url.protocol !== "https:") || url.origin !== text.replace(/\/$/, "")) throw new UsageError(`Invalid origin: ${text}`);
    return url.origin;
};

/** Reads the command line and the environment. A flag wins over its environment variable */
export const parseOptions = (argv: readonly string[], env: Readonly<Record<string, string | undefined>>): CliOptions => {
    let dataDir = env["STATENET_DATA_DIR"] ?? "./data";
    let host = env["STATENET_HOST"] ?? "127.0.0.1";
    let port = env["STATENET_PORT"] !== undefined ? parseInteger(env["STATENET_PORT"], "port", 0, 65535) : DEFAULT_PORT;
    let logLength = env["STATENET_LOG_LENGTH"] !== undefined ? parseInteger(env["STATENET_LOG_LENGTH"], "log length", 0, 1_000_000) : DEFAULT_LOG_LENGTH;
    const envOrigins = (env["STATENET_ALLOW_ORIGINS"] ?? "").split(",").map((origin) => origin.trim()).filter((origin) => origin !== "");
    let allowedOrigins = envOrigins.map(parseOrigin);
    let flagOrigins: string[] | undefined;
    let help = false;

    for (let index = 0; index < argv.length; index++) {
        const argument = argv[index] as string;
        const equals = argument.startsWith("--") ? argument.indexOf("=") : -1;
        const flag = equals < 0 ? argument : argument.slice(0, equals);
        const inline = equals < 0 ? undefined : argument.slice(equals + 1);
        const value = (): string => {
            if (inline !== undefined) return inline;
            const next = argv[++index];
            if (next === undefined) throw new UsageError(`${flag} needs a value`);
            return next;
        };
        switch (flag) {
            case "--data-dir":
                dataDir = value();
                break;
            case "--host":
                host = value();
                break;
            case "--port":
                port = parseInteger(value(), "port", 0, 65535);
                break;
            case "--log-length":
                logLength = parseInteger(value(), "log length", 0, 1_000_000);
                break;
            case "--allow-origin":
                flagOrigins = [...(flagOrigins ?? []), parseOrigin(value())];
                break;
            case "-h":
            case "--help":
                help = true;
                break;
            default:
                throw new UsageError(`Unknown option: ${argument}`);
        }
    }
    if (flagOrigins !== undefined) allowedOrigins = flagOrigins;
    return { dataDir: resolve(dataDir), host, port, logLength, allowedOrigins, help };
};
