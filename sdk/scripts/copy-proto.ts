// The SDK ships the API's .proto (the one source of truth is ../proto, shared with the server).
import { cpSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
cpSync(join(here, "..", "..", "proto"), join(here, "..", "proto"), { recursive: true });
