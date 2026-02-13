import ky from "https://esm.sh/ky@1.8.2";
import { chunk } from "jsr:@std/collections";
import { ensureDir } from "https://deno.land/std@0.224.0/fs/ensure_dir.ts";
const dataFolder = `temp`;
await ensureDir(`${dataFolder}/batches`);
const batchChunkSize = parseInt(Deno.env.get("BATCH_CHUNK_SIZE") || '100');
const { items } = await ky('https://mztrading.netlify.app/api/watchlist').json<{ items: { symbol: string }[] }>();

const batches = [] as string[];
const batchManifestFileName = `${dataFolder}/batch-manifest.json`;
const allSymbolsFileName = `${dataFolder}/all-symbols.json`;
if (items && items.length > 0) {
    const symbols = [...new Set(items.map(item => item.symbol))].sort().slice(0, 30);
    chunk(symbols, batchChunkSize).forEach((batch, index) => {
        //console.log(`Batch ${index + 1}}`);
        const batchFileName = `${dataFolder}/batches/batch-${index + 1}.json`;
        console.log(`Writing ${batchFileName} with ${batch.length} symbols...`);
        Deno.writeTextFileSync(`${batchFileName}`, JSON.stringify(batch, null, 2));
        batches.push(batchFileName);
    });
    console.log(`Writing batch manifest file: ${batchManifestFileName} with ${batches.length} batches...`);
    Deno.writeTextFileSync(`${batchManifestFileName}`, JSON.stringify(batches, null, 2));
    console.log(`Writing all symbols file: ${allSymbolsFileName} with ${symbols.length} symbols...`);
    Deno.writeTextFileSync(`${allSymbolsFileName}`, JSON.stringify(symbols, null, 2));
} else {
    throw new Error(`Unable to find any items in the watchlist!`);
}