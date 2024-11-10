import ky from "https://esm.sh/ky@1.2.3";
import { format } from "https://deno.land/std@0.224.0/datetime/format.ts";
import { ensureDir } from "https://deno.land/std@0.224.0/fs/ensure_dir.ts";
import { getOptionsDataSummary } from "../lib/data.ts";
const tickers = await ky("https://mztrading.netlify.app/api/watchlist").json<
    { items: { symbol: string; name: string }[] }
>();

console.log(`found ${tickers.items.length} tickers...`);
const items = tickers.items.slice(0, 3); //for testing work only with 3 items
const dataFolder = `temp/options-historical`;
const data = getOptionsDataSummary();
const releaseName = Deno.env.get("RELEASE_NAME") ||
    `OPTIONS_DATA_${format(new Date(), "yyyy-MM-dd HH:mm")}`;
data[releaseName] = {
    displayName: format(new Date(), "yyyy-MM-dd HH:mm"),
    created: new Date(),
    published: false,
    symbols: {},
};
for (const ticker of items) {
    const fileName = `${ticker.symbol}.json`;
    await ensureDir(dataFolder);
    const { raw } = await ky(
        `https://mztrading.netlify.app/api/symbols/${ticker.symbol}/options/analyze/tradier?dte=90&sc=30`,
        {
            retry: {
                limit: 10,
            },
        },
    ).json<{ raw: any }>();
    await Deno.writeTextFile(`${dataFolder}/${fileName}`, JSON.stringify(raw)); //it'll overwrite if it already exists
    data[releaseName].symbols[ticker.symbol] = {
        fileName: fileName,
        assetUrl: `https://github.com/mnsrulz/mztrading-data/releases/download/${releaseName}/${fileName}`,
    };
}

console.log(`Finished generating data files!`);

Deno.writeTextFileSync(
    "./data/options-data.summary.json",
    JSON.stringify(data),
);

console.log(`Summary file generated successfully!`);