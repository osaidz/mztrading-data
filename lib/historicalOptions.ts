
// @deno-types="https://esm.sh/v135/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.d.ts"
import { createDuckDB, getJsDelivrBundles, ConsoleLogger, DEFAULT_RUNTIME } from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.mjs/+esm';

import optionsRollingSummary from "./../data/cboe-options-rolling.json" with {
    type: "json",
};
import { getPriceAtDate } from './historicalPrice.ts';
import dayjs from "https://esm.sh/dayjs@1.11.13";

const logger = new ConsoleLogger();
const JSDELIVR_BUNDLES = getJsDelivrBundles();

const initialize = async () => {
    const { assetUrl, name } = optionsRollingSummary;
    // const ds = 'https://github.com/mnsrulz/mztrading-data/releases/download/archives/output_test_all.parquet';

    //HTTP paths are not supported due to xhr not available in deno.
    //db.registerFileURL('db.parquet', assetUrl, DuckDBDataProtocol.HTTP, false);

    console.log(`initializing duckdb with ${assetUrl} and name: ${name}`);
    const db = await createDuckDB(JSDELIVR_BUNDLES, logger, DEFAULT_RUNTIME);
    await db.instantiate(() => { });
    const arrayBuffer = await fetch(assetUrl)    //let's initialize the data set in memory
        .then(r => r.arrayBuffer());
    db.registerFileBuffer('db.parquet', new Uint8Array(arrayBuffer));
    return db;
}

const dbPromise = initialize();

export const getConnection = async () => {
    const dbPromiseVal = await dbPromise;
    return dbPromiseVal.connect();
}

//find a way to parametrize the query
export const getHistoricalSnapshotDatesFromParquet = async (symbol: string) => {
    const conn = await getConnection();
    const arrowResult = await conn.send("SELECT DISTINCT CAST(dt as STRING) as dt FROM 'db.parquet' WHERE option_symbol = '" + symbol + "'");
    return arrowResult.readAll()[0].toArray().map((row) => row.toJSON());
}

export const getHistoricalOptionDataFromParquet = async (symbol: string, dt: string) => {
    const conn = await getConnection();
    const arrowResult = await conn.send("SELECT cast(expiration as string) as expiration, delta, option_type, gamma, cast(strike as string) strike, open_interest, volume FROM 'db.parquet' WHERE option_symbol = '" + symbol + "' AND dt = '" + dt + "'");
    return arrowResult.readAll()[0].toArray().map((row) => row.toJSON());
}

export const lastHistoricalOptionDataFromParquet = () => {
    return optionsRollingSummary;
}

type MicroOptionContractItem = { oi: number, volume: number, delta: number, gamma: number }
type MicroOptionContract = { call: MicroOptionContractItem, put: MicroOptionContractItem }
type ExposureDataItem = { absDelta: number[], openInterest: number[], volume: number[] }
type ExposureDataType = { call: ExposureDataItem, put: ExposureDataItem, netGamma: number[], strikes: string[], expiration: string, dte: number }
export const getExposureData = async (symbol: string, dt: string) => {
    const spotDate = dayjs(dt).format('YYYY-MM-DD');
    const historicalData = await getHistoricalOptionDataFromParquet(symbol, dt);
    const indexedObject = historicalData.reduce((previous, current) => {
        previous[current.expiration] = previous[current.expiration] || {};
        previous[current.expiration][current.strike] = previous[current.expiration][current.strike] || {};
        //does it make sense to throw exception if delta/gamma values doesn't seem accurate? like gamma being negative or delta being greater than 1?
        if (current.option_type == 'C') {
            previous[current.expiration][current.strike].call = { oi: current.open_interest, volume: current.volume, delta: current.delta, gamma: current.gamma };
        } else if(current.option_type == 'P') {
            previous[current.expiration][current.strike].put = { oi: current.open_interest, volume: current.volume, delta: current.delta, gamma: current.gamma };
        } else {
            throw new Error("Invalid option type");
        }
        return previous;
    }, {} as Record<string, Record<string, MicroOptionContract>>)

    const _spotPrice = await getPriceAtDate(symbol, dt, true);
    if (!_spotPrice || Number.isNaN(_spotPrice)) throw new Error("Invalid spot price");
    const spotPrice = Number(_spotPrice);

    const dataToPersist = {
        data: [] as ExposureDataType[],
        spotPrice: spotPrice
    }

    const expirations = Object.keys(indexedObject);
    for (const expiration of expirations) {
        const strikes = Object.keys(indexedObject[expiration]);
        const callOpenInterestData = new Array<number>(strikes.length).fill(0);
        const putOpenInterestData = new Array<number>(strikes.length).fill(0);

        const callVolumeData = new Array<number>(strikes.length).fill(0);
        const putVolumeData = new Array<number>(strikes.length).fill(0);

        const callDeltaData = new Array<number>(strikes.length).fill(0);
        const putDeltaData = new Array<number>(strikes.length).fill(0);

        const netGammaData = new Array<number>(strikes.length).fill(0);

        for (let ix = 0; ix < strikes.length; ix++) {
            callOpenInterestData[ix] = (indexedObject[expiration][strikes[ix]]?.call?.oi || 0);
            putOpenInterestData[ix] = indexedObject[expiration][strikes[ix]]?.put?.oi || 0;

            callVolumeData[ix] = (indexedObject[expiration][strikes[ix]]?.call?.volume || 0);
            putVolumeData[ix] = indexedObject[expiration][strikes[ix]]?.put?.volume || 0;

            callDeltaData[ix] = Math.trunc((indexedObject[expiration][strikes[ix]]?.call?.delta || 0) * 100 * callOpenInterestData[ix] * spotPrice);
            putDeltaData[ix] = Math.trunc((indexedObject[expiration][strikes[ix]]?.put?.volume || 0) * 100 * putOpenInterestData[ix] * spotPrice);

            const callGamma = (indexedObject[expiration][strikes[ix]]?.call?.gamma || 0) * 100 * callOpenInterestData[ix] * spotPrice;
            const putGamma = (indexedObject[expiration][strikes[ix]]?.put?.gamma || 0) * 100 * putOpenInterestData[ix] * spotPrice;
            netGammaData[ix] = Math.trunc(callGamma - putGamma);
        }

        dataToPersist.data.push({
            call: {
                absDelta: callDeltaData,
                openInterest: callOpenInterestData,
                volume: callVolumeData
            },
            put: {
                absDelta: putDeltaData,
                openInterest: putOpenInterestData,
                volume: putVolumeData
            },
            netGamma: netGammaData,
            strikes: strikes,
            expiration,
            dte: dayjs(expiration).diff(spotDate, 'day')
        });
    }
    return dataToPersist;
}