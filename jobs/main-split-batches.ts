import { chunk } from "jsr:@std/collections";
import { getCboeLatestDateAndSymbols } from "../lib/data.ts";
import { ensureDir } from "https://deno.land/std@0.224.0/fs/ensure_dir.ts";
const dataFolder = `temp`;
await ensureDir(`${dataFolder}/batches`);

const forceDayId = Deno.env.get("FORCE_DAY_ID")

forceDayId && console.log(`Force day id for this release: ${forceDayId}`);
const latestDateAndSymbols = getCboeLatestDateAndSymbols(forceDayId);
const batches = [] as string[];
const batchManifestFileName = `${dataFolder}/batch-manifest.json`;
const allSymbolsFileName = `${dataFolder}/all-symbols.json`;
if (latestDateAndSymbols?.symbols && latestDateAndSymbols.symbols.length > 0) {
    chunk(latestDateAndSymbols.symbols, 100).forEach((batch, index) => {
        //console.log(`Batch ${index + 1}}`);
        const batchFileName = `${dataFolder}/batches/batch-${index + 1}.json`;
        console.log(`Writing ${batchFileName} with ${batch.length} symbols...`);
        Deno.writeTextFileSync(`${batchFileName}`, JSON.stringify(batch, null, 2));
        batches.push(batchFileName);
    });
    console.log(`Writing batch manifest file: ${batchManifestFileName} with ${batches.length} batches...`);
    Deno.writeTextFileSync(`${batchManifestFileName}`, JSON.stringify(batches, null, 2));
    console.log(`Writing all symbols file: ${allSymbolsFileName} with ${latestDateAndSymbols.symbols.length} symbols...`);
    Deno.writeTextFileSync(`${allSymbolsFileName}`, JSON.stringify(latestDateAndSymbols.symbols, null, 2));
} else {
    throw new Error(`Unable to find any latest date in the data summary file!`);
}