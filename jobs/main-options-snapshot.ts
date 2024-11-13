import { ensureDir } from "https://deno.land/std@0.224.0/fs/ensure_dir.ts";
import { format } from "https://deno.land/std@0.224.0/datetime/format.ts";
import puppeteer from "https://deno.land/x/puppeteer@16.2.0/mod.ts";
import ky from "https://esm.sh/ky@1.2.3";
import { getOptionsSnapshotSummary, ghRepoBaseUrl } from "../lib/data.ts";
const dataFolder = `temp`;
await ensureDir(dataFolder);
const data = getOptionsSnapshotSummary();

const releaseName = Deno.env.get("RELEASE_NAME") ||
    `DEX_GEX_SNAPSHOT_${format(new Date(), "yyyy-MM-dd")}`;
data[releaseName] = {
    displayName: format(new Date(), "yyyy-MM-dd"),
    created: new Date(),
    symbols: {},
};
const currentRelease = data[releaseName];

const tickers = await ky("https://mztrading.netlify.app/api/watchlist").json<
    { items: { symbol: string; name: string }[] }
>();

console.log(`found ${tickers.items.length} tickers...`);
const items = tickers.items; //.slice(0, 3); //for testing work only with 3 items
for (const ticker of items) {
    console.log(`Fetching dex/gex page for ${ticker.symbol}`);

    currentRelease.symbols[ticker.symbol] = {
        dex: {
            sdFileName: `${ticker.symbol}_DEX_620.png`,
            hdFileName: `${ticker.symbol}_DEX_1240.png`,
            sdAssetUrl: `${ghRepoBaseUrl}/${releaseName}/${ticker.symbol}_DEX_620.png`,
            hdAssetUrl: `${ghRepoBaseUrl}/${releaseName}/${ticker.symbol}_DEX_1240.png`
        },
        gex: {
            sdFileName: `${ticker.symbol}_GEX_620.png`,
            hdFileName: `${ticker.symbol}_GEX_1240.png`,
            sdAssetUrl: `${ghRepoBaseUrl}/${releaseName}/${ticker.symbol}_GEX_620.png`,
            hdAssetUrl: `${ghRepoBaseUrl}/${releaseName}/${ticker.symbol}_GEX_1240.png`
        }
    }

    const currentSymbol = currentRelease.symbols[ticker.symbol];

    const browser = await puppeteer.launch();
    const page = await browser.newPage();
    await page.setViewport({ width: 620, height: 620, deviceScaleFactor: 2 }); // set the viewport size
    await page.goto(
        `https://mztrading.netlify.app/options/analyze/${ticker.symbol}/dh?showDexGex=true&dgextab=DEX&print=true`,
        {
            waitUntil: "networkidle2",
        },
    ); // replace

    console.log(`Generating high definition DEX snapshot page for ${ticker.symbol}`);
    
    await page.waitForNetworkIdle();    
    await page.screenshot({
        path: `${dataFolder}/${currentSymbol.dex.hdFileName}`,
    }); // take a screenshot and save it to a file
    
    console.log(`Generating standard definition DEX snapshot page for ${ticker.symbol}`);
    await page.setViewport({ width: 620, height: 620, deviceScaleFactor: 1 }); // set the viewport size
    
    await page.waitForNetworkIdle();
    await page.screenshot({
        path: `${dataFolder}/${currentSymbol.dex.sdFileName}`,
    }); // take a screenshot and save it to a file
    
    await page.evaluate(`
        const url = new URL(location.href);
        url.searchParams.set("dgextab", "GEX");
        history.replaceState(null, "", url);
        `);        
    console.log(`Generating standard definition GEX snapshot page for ${ticker.symbol}`);
    
    await page.waitForNetworkIdle();
    await page.screenshot({
        path: `${dataFolder}/${currentSymbol.gex.sdFileName}`,
    }); // take a screenshot and save it to a file
    
    await page.setViewport({ width: 620, height: 620, deviceScaleFactor: 2 }); // set the viewport size
    console.log(`Generating high definition GEX snapshot page for ${ticker.symbol}`);

    await page.waitForNetworkIdle();
    await page.screenshot({
        path: `${dataFolder}/${currentSymbol.gex.hdFileName}`,
    }); // take a screenshot and save it to a file

    await browser.close();
}

console.log(`Finished generating snapshot files!`);

Deno.writeTextFileSync(
    "./data/options-snapshot.summary.json",
    JSON.stringify(data),
);

console.log(`Summary file generated successfully!`);
