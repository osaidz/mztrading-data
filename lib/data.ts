import Fuse from "https://esm.sh/fuse.js@7.0.0";

// import optionsDataSummary from "./../data/options-data.summary.json" with {
//     type: "json",
// };

import optionsSnapshotSummary from "./../data/options-snapshot.summary.json" with {
    type: "json",
};

import cboeOptionsSummary from "./../data/cboe-options-summary.json" with {
    type: "json",
};

import optionsRollingSummary from "./../data/cboe-options-rolling.json" with {
    type: "json",
};

import symbols from "./../data/symbols.json" with {
    type: "json",
};

// type OptionsDataSummary = Record<string, {
//     displayName: string;
//     created: Date | string;
//     symbols: Record<string, {
//         fileName: string;
//         assetUrl: string;
//     }>;
// }>;

type OptionsSnapshotSummaryFileType = {
    hdFileName: string;
    hdAssetUrl?: string;
    sdFileName: string;
    sdAssetUrl?: string;
};
type OptionsSnapshotSummary = Record<string, {
    displayName: string;
    created: Date | string;
    zipAssetUrl?: string;
    symbols: Record<string, {
        "gex": OptionsSnapshotSummaryFileType;
        "dex": OptionsSnapshotSummaryFileType;
    }>;
}>;

type CboeOptionSummaryType = {
    name: string,
    optionsAssetUrl: string
}

type TickerSymbol = { name: string, symbol: string }

// export const getOptionsDataSummary = () => {
//     return optionsDataSummary as OptionsDataSummary;
// };

export const getOptionsSnapshotSummary = () => {
    return optionsSnapshotSummary as OptionsSnapshotSummary;
};

export const OptionsSnapshotSummary = (optionsSnapshotSummary as OptionsSnapshotSummary);

export const AvailableSnapshotDates = Object.values(OptionsSnapshotSummary).map(k => ({ dt: k.displayName }));

export const OptionsSnapshotSummaryLegacy = Object.fromEntries(Object.keys(OptionsSnapshotSummary).map(j => [OptionsSnapshotSummary[j].displayName, { zipAssetUrl: OptionsSnapshotSummary[j].zipAssetUrl, symbols: OptionsSnapshotSummary[j].symbols }]));

export const zipServiceUrl = 'https://zipservice-deno.deno.dev/download';//?f=AAOI_GEX_620.png&q=https://github.com/mnsrulz/mztrading-data/releases/download/DEX_GEX_SNAPSHOT_2025-07-08/options-snapshots.zip';
const snapshotCdnUrl = 'https://mztradingsnapshotcdn.deno.dev/api/snapshots'; //?dt=2025-08-14&f=AAPL_DEX_620.png&symbol=AAPL'
export const getSnapshotsAvailableForDate = (dt: string) => {
    const result = Object.values(OptionsSnapshotSummary).find(k => k.displayName == dt);
    if (result) {
        return Object.keys(result.symbols).map(k => {            
            return {
                symbol: k,
                dex: {
                    hdAssetUrl: result.zipAssetUrl ? `${snapshotCdnUrl}?f=${result.symbols[k].dex.hdFileName}&dt=${dt}&symbol=${k}` : result.symbols[k].dex.hdAssetUrl,
                    sdAssetUrl: result.zipAssetUrl ? `${snapshotCdnUrl}?f=${result.symbols[k].dex.sdFileName}&dt=${dt}&symbol=${k}` : result.symbols[k].dex.sdAssetUrl
                },
                gex: {
                    hdAssetUrl: result.zipAssetUrl ? `${snapshotCdnUrl}?f=${result.symbols[k].gex.hdFileName}&dt=${dt}&symbol=${k}` : result.symbols[k].gex.hdAssetUrl,
                    sdAssetUrl: result.zipAssetUrl ? `${snapshotCdnUrl}?f=${result.symbols[k].gex.sdFileName}&dt=${dt}&symbol=${k}` : result.symbols[k].gex.sdAssetUrl
                },
            }
        });
    }
    throw new Error('No data found for this date');
}

export const getZipAssetUrlForSymbol = (symbol: string, dt: string) => { 
    const result = Object.values(OptionsSnapshotSummary).find(k => k.displayName == dt);
    if (result) {
        if (result.symbols[symbol]) {
            return result.zipAssetUrl;
        } else {
            throw new Error(`No data found for symbol ${symbol} on date ${dt}`);
        }
    }
    throw new Error(`No data found for date ${dt}`);
}

export const getSnapshotsAvailableForSymbol = (symbol: string) => {
    const result = Object.keys(OptionsSnapshotSummaryLegacy)
        .filter((j) =>
            Object.keys(OptionsSnapshotSummaryLegacy[j].symbols).includes(
                symbol,
            )
        )
        .map((k) => ({ date: k, data: OptionsSnapshotSummaryLegacy[k].symbols[symbol], zipAssetUrl: OptionsSnapshotSummaryLegacy[k].zipAssetUrl }))
        .map(({ data, date, zipAssetUrl }) => ({
            date: date,
            dex: {
                hdAssetUrl: zipAssetUrl ? `${snapshotCdnUrl}?f=${data.dex.hdFileName}&dt=${date}&symbol=${symbol}` : data.dex.hdAssetUrl,
                sdAssetUrl: zipAssetUrl ? `${snapshotCdnUrl}?f=${data.dex.sdFileName}&dt=${date}&symbol=${symbol}` : data.dex.sdAssetUrl
            },
            gex: {
                hdAssetUrl: zipAssetUrl ? `${snapshotCdnUrl}?f=${data.gex.hdFileName}&dt=${date}&symbol=${symbol}` : data.gex.hdAssetUrl,
                sdAssetUrl: zipAssetUrl ? `${snapshotCdnUrl}?f=${data.gex.sdFileName}&dt=${date}&symbol=${symbol}` : data.gex.sdAssetUrl
            },
        }));
    return result;
}

// export const mapDataToLegacy = () => {
//     const intermediateData = getOptionsDataSummary();
//     return Object.keys(intermediateData).flatMap((j) => {
//         return Object.keys(intermediateData[j].symbols).map((k) => ({
//             symbol: k,
//             dt: intermediateData[j].displayName,
//         }));
//     });
// };

export const ghRepoBaseUrl = 'https://github.com/mnsrulz/mztrading-data/releases/download';

export const cleanSymbol = (symbol: string) => decodeURIComponent(symbol).replace(/\W/g, '');

const allTickerSymbols = symbols as TickerSymbol[]

const fuse = new Fuse(allTickerSymbols, {
    keys: ["symbol", "name"],
    threshold: 0.2,
});

export const searchTicker = (q: string) => {
    const filtered = fuse.search(q, { limit: 25 }).map((x) => x.item);
    return filtered;
}

export const CboeOptionsRawSummary = (cboeOptionsSummary as CboeOptionSummaryType[]).map(({ name, optionsAssetUrl }) => ({ name, optionsAssetUrl, dt: name.replace('CBOE_OPTIONS_DATA_', '').substring(0, 10) }));

export const getCboeLatestDateAndSymbols = (forceDayId?: string) => {
    if (forceDayId) {
        if (optionsRollingSummary.symbolsSummary.some(k => k.dt == forceDayId)) {
            return {
                latestDate: forceDayId,
                symbols: optionsRollingSummary.symbolsSummary.filter(k => k.dt == forceDayId).map(k => k.symbol)    //.slice(0, 30) // Limit to 30 symbols for testing
            }
        } else {
            return null;
        }
    }

    const latestDate = optionsRollingSummary.symbolsSummary.map(k => k.dt).sort().pop();
    if (latestDate) {
        return {
            latestDate,
            symbols: optionsRollingSummary.symbolsSummary.filter(k => k.dt == latestDate).map(k => k.symbol)
        }
    }
    return null;
} 
