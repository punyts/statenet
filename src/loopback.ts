const LOOPBACK_NAMES = new Set(["localhost", "::1", "[::1]"]);
const IPV4_LOOPBACK = /^127\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/;

/** A loopback address or name: `localhost`, `::1` or 127.0.0.0/8 */
export const isLoopbackHost = (host: string): boolean => {
    const name = host.toLowerCase();
    if (LOOPBACK_NAMES.has(name)) return true;
    const match = IPV4_LOOPBACK.exec(name);
    return match !== null && match.slice(1).every((octet) => Number(octet) <= 255);
};

/** The host name of a `Host` header, without the port; undefined when there is none */
export const hostNameOf = (header: string | undefined): string | undefined => {
    if (header === undefined || header === "") return undefined;
    if (header.startsWith("[")) {
        const end = header.indexOf("]");
        return end < 0 ? undefined : header.slice(0, end + 1);
    }
    const colon = header.indexOf(":");
    return colon < 0 ? header : header.slice(0, colon);
};
