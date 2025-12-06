import { ensureDir } from "https://deno.land/std@0.224.0/fs/ensure_dir.ts";
import { format } from "https://deno.land/std@0.224.0/datetime/format.ts";
import puppeteer, { Page, Browser } from "https://deno.land/x/puppeteer@16.2.0/mod.ts";
import pretry from "https://esm.sh/p-retry@6.2.1";
import pMap from "https://esm.sh/p-map@7.0.3";
import pTimeout from "https://esm.sh/p-timeout@6.1.4";
import { chunk } from "jsr:@std/collections";
import { nanoid } from "https://esm.sh/nanoid@5.1.5";
import delay from "https://esm.sh/delay@6.0.0";
const dop = 4;  //degree of parallelism, how many symbols to process in parallel

import { getOptionsSnapshotSummary, ghRepoBaseUrl, cleanSymbol, getCboeLatestDateAndSymbols } from "../lib/data.ts";
const dataFolder = `temp/options-snapshots`;
await ensureDir(dataFolder);
const data = getOptionsSnapshotSummary();
let pageFetchCounter = 0;
const timeoutInMS = 3000;
const batchFileName = Deno.env.get("BATCH_FILE");

if(!batchFileName) {
    throw new Error(`BATCH_FILE environment variable is not set!`);
}
console.log(`🔄 Generating options snapshot for batch file: ${batchFileName}`);

const batchContent = Deno.readTextFileSync(batchFileName);
const symbols: string[] = JSON.parse(batchContent);

console.log(`Processing ${symbols.length} symbols from batch file: ${batchFileName}...`);

console.log(`${JSON.stringify(symbols, null, 2)}`);

console.log(`🟢 Finished generating snapshot files!`);