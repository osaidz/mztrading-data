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
const releaseName = Deno.env.get("RELEASE_NAME") ||
    `DEX_GEX_SNAPSHOT_${format(new Date(), "yyyy-MM-dd")}`;
const forceDayId = Deno.env.get("FORCE_DAY_ID")

forceDayId && console.log(`Force day id for this release: ${forceDayId}`);

console.log(`🔄 Generating options snapshot for release: ${releaseName}`);

data[releaseName] = {
    displayName: forceDayId || format(new Date(), "yyyy-MM-dd"),
    created: new Date(),
    zipAssetUrl: `${ghRepoBaseUrl}/${releaseName}/options-snapshots.zip`,
    symbols: {},
};
const currentRelease = data[releaseName];

const latestDateAndSymbols = getCboeLatestDateAndSymbols(forceDayId);
let totalSymbols = 0;
let processingCounter = 0;
const runners = [];
if (latestDateAndSymbols && latestDateAndSymbols.latestDate) {
    console.log(`Latest date: ${latestDateAndSymbols.latestDate}`);
    const allSymbols = latestDateAndSymbols.symbols;//.slice(0, 30); //for testing work only with 3 items
    console.log(`Found ${allSymbols.length} tickers...`);
    totalSymbols = allSymbols.length;

    const chunkSize = Math.ceil(allSymbols.length / dop);  //keep it small to avoid any issues with puppeteer?
    console.log(`chunking symbols with ${chunkSize} size...`);
    const batches = chunk(allSymbols, chunkSize);
    console.log(`Processing in ${batches.length} batches with ${chunkSize} symbols each and dop ${dop}...`);
    console.log(`Total symbols to process: ${totalSymbols}`);
    await pMap(batches, processBatch, {
        concurrency: dop
    });

    console.log(`🟢 Finished generating snapshot files!`);
    console.log(`🟢 Page fetch count: ${pageFetchCounter}`);

    Deno.writeTextFileSync(
        "./data/options-snapshot.summary.json",
        JSON.stringify(data, null, 2),
    );

    console.log(`🟢 Summary file generated successfully!`);
} else {
    throw new Error(`Unable to find any latest date in the data summary file!`);
}

async function processBatch(batchSymbols: string[]) {
    const batchId = nanoid(10);
    console.log(`🚗 Processing batch ${batchId} with ${batchSymbols.length} symbols...`);
    runners.push(batchId);
    let browser: Browser;
    let page: Page;
    async function initializePage() {
        console.log(`🔄 Initializing page for batch ${batchId}...`);
        if (browser) {
            console.log(`🔄 Closing existing browser instance for batch ${batchId}...`);
            await browser.close();
            console.log(`🔄 Closed existing browser instance for batch ${batchId}...`);
        }
        browser = await puppeteer.launch();
        page = await browser.newPage();
        await pretry(async (n: number) => {
            if (n > 1) console.warn(`🚧 Batch: ${batchId} - ProcessBatch initial page navigation retry attempt: ${n}`);
            pageFetchCounter++;
            await page.goto(
                `https://mztrading.netlify.app/tools/snapshot?dgextab=DEX&print=true&mode=HISTORICAL&historical=${encodeURIComponent(latestDateAndSymbols.latestDate)}`,
                {
                    waitUntil: "networkidle2",
                },
            ); // replace

            await delay(5000); // wait for a few seconds to ensure the page is loaded properly
        }, {
            retries: 3
        });
    }

    await initializePage();

    for (const symbol of batchSymbols) {
        processingCounter++;
        console.log(`🔄 Processing symbol: ${symbol} in batch ${batchId}. Progress: ${processingCounter}/${totalSymbols}`);
        await pretry(async (n: number) => {
            if (n > 1) {
                console.warn(`🚧 Retrying the batch: ${batchId}, attempt: ${n}/3`);
                await initializePage();
            }
            await processSymbol(page, batchSymbols, symbol, batchId);
        }, {
            retries: 3
        })
    }
    if (browser) {
        await browser.close();
    }
    console.log(`🚀 Finished processing batch ${batchId} with ${batchSymbols.length} symbols...`);
}

async function processSymbol(page: Page, allSymbols: string[], symbol: string, batchId: string) {
    async function captureScreenshot(path: string) {
        async function captureScreenshotCore(){
            console.log(`⏳ ${symbol} - captureScreenshot - waiting for network idle...`);
            await page.waitForNetworkIdle({
                timeout: timeoutInMS
            });
            
            console.log(`📷 ${symbol} - Taking screenshot and saving to ${path}`);
            await page.screenshot({
                path: path
            }); // take a screenshot and save it to a file
            
            console.log(`⚡ ${symbol} - Screenshot saved successfully to path ${path}`);
        }

        await pTimeout(captureScreenshotCore(), {
            milliseconds: timeoutInMS
        });
    }

    const cleanedSymbol = cleanSymbol(symbol)

    console.log(`🎁  ${symbol} - Batch - ${batchId}. Fetching dex/gex page`);

    currentRelease.symbols[symbol] = {
        dex: {
            sdFileName: `${cleanedSymbol}_DEX_620.png`,
            hdFileName: `${cleanedSymbol}_DEX_1240.png`,
        },
        gex: {
            sdFileName: `${cleanedSymbol}_GEX_620.png`,
            hdFileName: `${cleanedSymbol}_GEX_1240.png`
        }
    }

    const currentSymbol = currentRelease.symbols[symbol];

    await page.setViewport({ width: 620, height: 620, deviceScaleFactor: 2 }); // set the viewport size
    let varurlname = `urldex${allSymbols.indexOf(symbol)}${new Date().getTime()}`
    const scriptToRun = `
            let ${varurlname} = new URL(location.href);
            ${varurlname}.searchParams.set("dgextab", "DEX");
            ${varurlname}.searchParams.set("symbol", "${symbol}");
            history.replaceState(null, "", ${varurlname});
        `;
    // console.log(`Script: ${scriptToRun}`);
    await page.evaluate(scriptToRun);

    console.log(`⬆️ ${symbol} - Generating high definition DEX snapshot page`);

    await page.waitForNetworkIdle({
        timeout: timeoutInMS
    });
    await captureScreenshot(`${dataFolder}/${currentSymbol.dex.hdFileName}`); // take a screenshot and save it to a file

    console.log(`⬆️ ${symbol} - Generating standard definition DEX snapshot page`);
    await page.setViewport({ width: 620, height: 620, deviceScaleFactor: 1 }); // set the viewport size

    await captureScreenshot(`${dataFolder}/${currentSymbol.dex.sdFileName}`); // take a screenshot and save it to a file

    varurlname = `urlgex${allSymbols.indexOf(symbol)}${new Date().getTime()}`
    const scriptToRunGex = `
        let ${varurlname} = new URL(location.href);
        ${varurlname}.searchParams.set("dgextab", "GEX");
        history.replaceState(null, "", ${varurlname});
        `;
    await page.evaluate(scriptToRunGex);
    console.log(`⬆️ ${symbol} - Generating standard definition GEX snapshot page`);

    await captureScreenshot(`${dataFolder}/${currentSymbol.gex.sdFileName}`); // take a screenshot and save it to a file

    await page.setViewport({ width: 620, height: 620, deviceScaleFactor: 2 }); // set the viewport size
    console.log(`⬆️ ${symbol} - Generating high definition GEX snapshot page`);

    await captureScreenshot(`${dataFolder}/${currentSymbol.gex.hdFileName}`); // take a screenshot and save it to a file      

    console.log(`✅ Finished processing symbol: ${symbol}`);
}

