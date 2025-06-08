import { getOIAnomalyConnection } from "./historicalOptions.ts";

export type OIAnomalySearchRequest = {
    params: {
        facets: string | string[],
        facetFilters: [string[]],
        query: string,
        page: number,
        hitsPerPage: number,
    }
}
export type OIAnomalySearchResponse = {
    results: {
        hits: {}[],
        // nbHits: number,
        // nbPages: number,
        // page: number,
        // hitsPerPage: number,
        // exhaustiveNbHits: boolean,
        // exhaustiveFacetsCount: boolean,
        // processingTimeMS: number,
        facets: Record<string, { name: string, count: number }[]>,
        // facetFilters: string[],
        // query: string,
        // params: string,
        // index: string,
        // exhaustiveFacets: boolean,
        // exhaustiveAttributesCount: boolean,
    }[]
}

export const queryOIAnomalySearch = async (request: OIAnomalySearchRequest[]): Promise<OIAnomalySearchResponse> => {
    const conn = await getOIAnomalyConnection();
    const facetValues: Record<string, any> = {};
    let hits = [] as any;

    for (const req of request) {
        const { params } = req;
        if (typeof params.facets === 'string') {

            const query = params.facetFilters && params.facetFilters.map(f => {
                f.map(k => {
                    const [key, value] = k.split(':');
                    return `${key} = '${value}'`;
                }).join(' OR ');
            }).join(' AND ');
            console.log(`executing query: ${query}`);
            const result = await conn.send(`SELECT ${params.facets}, COUNT(1) FROM 'oianomaly.parquet' 
                ${query && 'WHERE ' + query} 
                GROUP BY ${params.facets}`);
            facetValues[params.facets] = result;
        }
        else if (Array.isArray(params.facets)) {
            const query = params.facetFilters && params.facetFilters.map(f => {
                f.map(k => {
                    const [key, value] = k.split(':');
                    return `${key} = '${value}'`;
                }).join(' OR ');
            }).join(' AND ');
            console.log(`executing query: ${query}`);

            const arrowResult = await conn.send(`
                SELECT CAST(dt as STRING) as dt, 
                option, option_symbol, 
                CAST(expiration as STRING) as expiration, 
                dte, option_type, 
                strike, 
                delta, 
                gamma,
                open_interest, 
                volume, 
                prev_open_interest, 
                oi_change, 
                anomaly_score 
                FROM 'oianomaly.parquet'
                ${query && 'WHERE ' + query}
                ORDER BY 
                anomaly_score desc
                LIMIT 1000
            `);
            hits = arrowResult.readAll().flatMap(k => k.toArray().map((row) => row.toJSON())) as {
                dt: string, option: string, option_symbol: string, expiration: string, dte: number, strike: number, delta: number, gamma: number,
                open_interest: number, volume: number, prev_open_interest: number, oi_change: number, anomaly_score: number
            }[];
        }
    }

    return {
        results: [
            {
                hits: hits,
                facets: facetValues,
                // page: 0,
                // nbHits: 123,
                // nbPages: Math.ceil(123 / 20),
                // hitsPerPage: 20,
                // processingTimeMS: 100,


                // exhaustiveFacetsCount: true,
                // exhaustiveNbHits: true,
                // query: (searchMethodParams && searchMethodParams[0] && searchMethodParams[0].params && searchMethodParams[0].params.query) || "",
                // params: (searchMethodParams && searchMethodParams[0] && searchMethodParams[0].params) ? Object.entries(searchMethodParams[0].params).map(([k, v]) => `${k}=${v}`).join('&') : ""
            }
        ]
    }
}