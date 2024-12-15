import { ensureDir } from "https://deno.land/std@0.224.0/fs/ensure_dir.ts";
import { format } from "https://deno.land/std@0.224.0/datetime/format.ts";
import puppeteer from "https://deno.land/x/puppeteer@16.2.0/mod.ts";
import ky from "https://esm.sh/ky@1.2.3";
import { getOptionsDataSummary, getOptionsSnapshotSummary, ghRepoBaseUrl, cleanSymbol } from "../lib/data.ts";
const dataFolder = `temp/options-snapshots`;
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

const dataSummary = getOptionsDataSummary()
const latestObject = Object.keys(dataSummary).pop() || '';
const latestDate = dataSummary[latestObject].displayName
if (latestDate) {
    const allSymbols = Object.keys(dataSummary[latestObject].symbols);//.slice(0, 30); //for testing work only with 3 items
    console.log(`found ${allSymbols.length} tickers...`);
    const browser = await puppeteer.launch();
    const page = await browser.newPage();    
    await page.goto(
        `https://mztrading.netlify.app/tools/snapshot?showDexGex=true&dgextab=DEX&print=true&datamode=${encodeURIComponent(latestDate)}`,
        {
            waitUntil: "networkidle2",
        },
    ); // replace

        
    for (const symbol of allSymbols) {
        const cleanedSymbol = cleanSymbol(symbol)
        console.log(`Fetching dex/gex page for ${symbol}`);

        currentRelease.symbols[symbol] = {
            dex: {
                sdFileName: `${cleanedSymbol}_DEX_620.png`,
                hdFileName: `${cleanedSymbol}_DEX_1240.png`,
                sdAssetUrl: `${ghRepoBaseUrl}/${releaseName}/${cleanedSymbol}_DEX_620.png`,
                hdAssetUrl: `${ghRepoBaseUrl}/${releaseName}/${cleanedSymbol}_DEX_1240.png`
            },
            gex: {
                sdFileName: `${cleanedSymbol}_GEX_620.png`,
                hdFileName: `${cleanedSymbol}_GEX_1240.png`,
                sdAssetUrl: `${ghRepoBaseUrl}/${releaseName}/${cleanedSymbol}_GEX_620.png`,
                hdAssetUrl: `${ghRepoBaseUrl}/${releaseName}/${cleanedSymbol}_GEX_1240.png`
            }
        }

        const currentSymbol = currentRelease.symbols[symbol];
        
        await page.setViewport({ width: 620, height: 620, deviceScaleFactor: 2 }); // set the viewport size
        let varurlname = `urldex${allSymbols.indexOf(symbol)}`
        await page.evaluate(`
            let ${varurlname} = new URL(location.href);
            ${varurlname}.searchParams.set("dgextab", "DEX");
            ${varurlname}.searchParams.set("symbol", "${symbol}");
            history.replaceState(null, "", ${varurlname});
        `);

        console.log(`Generating high definition DEX snapshot page for ${symbol}`);

        await page.waitForNetworkIdle();
        await page.screenshot({
            path: `${dataFolder}/${currentSymbol.dex.hdFileName}`
        }); // take a screenshot and save it to a file

        console.log(`Generating standard definition DEX snapshot page for ${symbol}`);
        await page.setViewport({ width: 620, height: 620, deviceScaleFactor: 1 }); // set the viewport size
        
        await page.waitForNetworkIdle();
        await page.screenshot({
            path: `${dataFolder}/${currentSymbol.dex.sdFileName}`,
        }); // take a screenshot and save it to a file
        
        varurlname = `urlgex${allSymbols.indexOf(symbol)}`
        await page.evaluate(`
        let ${varurlname} = new URL(location.href);
        ${varurlname}.searchParams.set("dgextab", "GEX");
        history.replaceState(null, "", ${varurlname});
        `);
        console.log(`Generating standard definition GEX snapshot page for ${symbol}`);

        await page.waitForNetworkIdle();
        await page.screenshot({
            path: `${dataFolder}/${currentSymbol.gex.sdFileName}`,
        }); // take a screenshot and save it to a file

        await page.setViewport({ width: 620, height: 620, deviceScaleFactor: 2 }); // set the viewport size
        console.log(`Generating high definition GEX snapshot page for ${symbol}`);

        await page.waitForNetworkIdle();
        await page.screenshot({
            path: `${dataFolder}/${currentSymbol.gex.hdFileName}`,
        }); // take a screenshot and save it to a file

        // await browser.close();
    }
    await browser.close();
    console.log(`Finished generating snapshot files!`);

    Deno.writeTextFileSync(
        "./data/options-snapshot.summary.json",
        JSON.stringify(data),
    );

    console.log(`Summary file generated successfully!`);
} else {
    console.log(`Unable to find any latest date in the data summary file!`);
}
