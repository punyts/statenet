#!/usr/bin/env node
import { addListener, setCategories } from "@punyts/core";
import { HELP_TEXT, parseOptions, UsageError } from "./options.js";
import { startStatenet } from "./Statenet.js";

const main = async (): Promise<void> => {
    let options;
    try {
        options = parseOptions(process.argv.slice(2), process.env);
    } catch (error) {
        if (error instanceof UsageError) {
            process.stderr.write(`${error.message}\n\n${HELP_TEXT}`);
            process.exitCode = 2;
            return;
        }
        throw error;
    }
    if (options.help) {
        process.stdout.write(HELP_TEXT);
        return;
    }

    // The statenet and state-sync report namespaces, revisions and errors, never change values
    setCategories(["statenet", "state-sync", "warning", "error"]);
    addListener((_timestamp, category, message) => {
        // The reporter has put the details into the message already
        process.stderr.write(`[${category}] ${message}\n`);
    });

    const statenet = await startStatenet({
        host: options.host,
        port: options.port,
        dataDir: options.dataDir,
        logLength: options.logLength,
        allowedOrigins: options.allowedOrigins
    });
    process.stderr.write(`statenet: ${statenet.url}  data in ${options.dataDir}\n`);

    let stopping = false;
    const stop = (): void => {
        if (stopping) return;
        stopping = true;
        void statenet.close().finally(() => process.exit(0));
    };
    process.on("SIGINT", stop);
    process.on("SIGTERM", stop);
};

main().catch((error: unknown) => {
    process.stderr.write(`statenet: ${error instanceof Error ? error.message : String(error)}\n`);
    process.exit(1);
});
