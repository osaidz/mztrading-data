import { ensureDir } from "https://deno.land/std@0.224.0/fs/ensure_dir.ts";
import puppeteer, { Browser, Page } from "https://deno.land/x/puppeteer@16.2.0/mod.ts";
import pretry from "https://esm.sh/p-retry@6.2.1";
import { cleanSymbol, getCboeLatestDateAndSymbols } from "../../lib/data.ts";

const MATRIX_ID = Deno.env.get("MATRIX_ID");
const dataFolder = `temp/options-snapshots/batch-${MATRIX_ID}`;
await ensureDir(dataFolder);
const timeoutInMS = 10000;
const batchFileName = Deno.env.get("BATCH_FILE");

if (!batchFileName) {
    throw new Error(`BATCH_FILE environment variable is not set!`);
}
console.log(`🔄 Generating options snapshot for batch file: ${batchFileName}`);

const batchContent = Deno.readTextFileSync(batchFileName);
const symbols: string[] = JSON.parse(batchContent);

console.log(`Processing ${symbols.length} symbols from batch file: ${batchFileName}...`);

console.log(`${JSON.stringify(symbols, null, 2)}`);

const forceDayId = Deno.env.get("FORCE_DAY_ID")
forceDayId && console.log(`Force day id for this release: ${forceDayId}`);
const latestDateAndSymbols = getCboeLatestDateAndSymbols(forceDayId);

if (latestDateAndSymbols == null || latestDateAndSymbols.latestDate == null) {
    throw new Error(`Unable to find any latest date in the data summary file!`);
}

let browser: Browser;
let page: Page;

async function initializePage() {
    browser = await puppeteer.launch();
    page = await browser.newPage();

    await page.goto(
        `https://mztrading.netlify.app/tools/snapshot?dgextab=DEX&print=true&mode=HISTORICAL&historical=${encodeURIComponent(latestDateAndSymbols.latestDate)}`,
        {
            waitUntil: "networkidle2",
        },
    );
    await page.waitForSelector('[data-testid="EXPOSURE-TOOLS"]', { visible: true, timeout: timeoutInMS });
}

await initializePage();

for (const symbol of symbols) {
    console.log(`Processing symbol: ${symbol}...`);
    await pretry(async (n: number) => {
        if (n > 1) {
            console.warn(`♻️ Retrying processing symbol: ${symbol}. Attempt #${n}`);
            await initializePage();
        }
        try {
            await processSymbol(symbol);
        } catch (err) {
            console.error(`❌ Error processing symbol ${symbol}: ${(err as Error).message}`);
            await browser.close();
            console.log(`Closed browser after error...`);
            throw err;
        }
    }, {
        retries: 3
    });
}

if (browser) {
    await browser.close();
}
console.log(`🚀 Finished generating snapshot files with ${symbols.length} symbols...`);

async function processSymbol(symbol: string) {
    async function captureScreenshot(path: string) {
        // async function captureScreenshotCore() {
        //     console.log(`⏳ ${symbol} - captureScreenshot - waiting for network idle...`);
        //     await page.waitForNetworkIdle({
        //         timeout: timeoutInMS
        //     });

        //     console.log(`📷 ${symbol} - Taking screenshot and saving to ${path}`);

        //     console.log(`⚡ ${symbol} - Screenshot saved successfully to path ${path}`);
        // }

        // await page.waitForSelector('');

        await page.screenshot({
            path: path
        }); // take a screenshot and save it to a file

        // await pTimeout(captureScreenshotCore(), {
        //     milliseconds: timeoutInMS
        // });
    }

    const cleanedSymbol = cleanSymbol(symbol)

    console.log(`🎁  ${symbol} - Fetching dex/gex page`);

    const currentSymbol = {
        dex: {
            sdFileName: `${cleanedSymbol}_DEX_620.png`,
            hdFileName: `${cleanedSymbol}_DEX_1240.png`,
        },
        gex: {
            sdFileName: `${cleanedSymbol}_GEX_620.png`,
            hdFileName: `${cleanedSymbol}_GEX_1240.png`
        }
    }

    // const currentSymbol = currentRelease.symbols[symbol];

    await page.setViewport({ width: 620, height: 620, deviceScaleFactor: 2 }); // set the viewport size
    let varurlname = `urldex${symbols.indexOf(symbol)}${new Date().getTime()}`
    const scriptToRun = `
            let ${varurlname} = new URL(location.href);
            ${varurlname}.searchParams.set("dgextab", "DEX");
            ${varurlname}.searchParams.set("symbol", "${symbol}");
            history.replaceState(null, "", ${varurlname});
        `;
    // console.log(`Script: ${scriptToRun}`);
    await page.evaluate(scriptToRun);

    console.log(`⬆️ ${symbol} - Generating high definition DEX snapshot page`);

    // await page.waitForNetworkIdle({
    //     timeout: timeoutInMS
    // });
    await page.waitForSelector(`[data-testid="EXPSOURE-CHART-${symbol}-DEX"]`, { visible: true, timeout: timeoutInMS });

    await captureScreenshot(`${dataFolder}/${currentSymbol.dex.hdFileName}`); // take a screenshot and save it to a file

    console.log(`⬆️ ${symbol} - Generating standard definition DEX snapshot page`);
    await page.setViewport({ width: 620, height: 620, deviceScaleFactor: 1 }); // set the viewport size

    await captureScreenshot(`${dataFolder}/${currentSymbol.dex.sdFileName}`); // take a screenshot and save it to a file

    varurlname = `urlgex${symbols.indexOf(symbol)}${new Date().getTime()}`
    const scriptToRunGex = `
        let ${varurlname} = new URL(location.href);
        ${varurlname}.searchParams.set("dgextab", "GEX");
        history.replaceState(null, "", ${varurlname});
        `;
    await page.evaluate(scriptToRunGex);
    console.log(`⬆️ ${symbol} - Generating standard definition GEX snapshot page`);

    await page.waitForSelector(`[data-testid="EXPSOURE-CHART-${symbol}-GEX"]`, { visible: true, timeout: timeoutInMS });

    await captureScreenshot(`${dataFolder}/${currentSymbol.gex.sdFileName}`); // take a screenshot and save it to a file

    await page.setViewport({ width: 620, height: 620, deviceScaleFactor: 2 }); // set the viewport size
    console.log(`⬆️ ${symbol} - Generating high definition GEX snapshot page`);

    await captureScreenshot(`${dataFolder}/${currentSymbol.gex.hdFileName}`); // take a screenshot and save it to a file      

    console.log(`✅ Finished processing symbol: ${symbol}`);
}


