import { getOIAnomalyConnection } from "./historicalOptions.ts";

export type OIAnomalySearchRequest = {
    params: {
        facets: string | string[],
        facetFilters: [string[]],
        query: string,
        page: number,
        hitsPerPage: number,
        numericFilters: string[]
    }
}
export type OIAnomalySearchResponse = {
    results: {
        hits: {}[],
        nbHits: number,
        nbPages: number,
        page: number,
        hitsPerPage: number,
        // exhaustiveNbHits: boolean,
        // exhaustiveFacetsCount: boolean,
        processingTimeMS: number,
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

    const mainRequest = request.find(k => Array.isArray(k.params.facets));
    if (!mainRequest) throw new Error(`no main request found!`);
    const availableFacets = mainRequest.params.facets as string[];
    const hits = await executeMainQuery(mainRequest);
    const facetValues = await executeFacet(request, mainRequest);

    return {
        results: [
            {
                hits: hits,
                facets: facetValues,
                page: 0,
                nbHits: 10,
                nbPages: 1,
                hitsPerPage: 20,
                processingTimeMS: 100,
                // exhaustiveFacetsCount: true,
                // exhaustiveNbHits: true,
                // query: (searchMethodParams && searchMethodParams[0] && searchMethodParams[0].params && searchMethodParams[0].params.query) || "",
                // params: (searchMethodParams && searchMethodParams[0] && searchMethodParams[0].params) ? Object.entries(searchMethodParams[0].params).map(([k, v]) => `${k}=${v}`).join('&') : ""
            }
        ]
    }
}

async function executeMainQuery(request: OIAnomalySearchRequest) {
    const conn = await getOIAnomalyConnection();
    let query = request.params.facetFilters && request.params.facetFilters.map(f => {
        const innerk = f.map(k => {
            const [key, value] = k.split(':');
            return `${key} = '${value}'`;
        }).join(' OR ');
        if (innerk) {
            return `(${innerk})`
        }
    }).join(' AND ');

    if (request.params.numericFilters && request.params.numericFilters.length > 0) {
        const numericQuery = request.params.numericFilters.join(' AND ');
        query = query ? `${query} AND ${numericQuery}` : numericQuery;
    }

    console.log(`executing main query: ${query} `);

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
                LIMIT 10
            `);
    return arrowResult.readAll().flatMap(k => k.toArray().map((row) => row.toJSON())) as {
        dt: string, option: string, option_symbol: string, expiration: string, dte: number, strike: number, delta: number, gamma: number,
        open_interest: number, volume: number, prev_open_interest: number, oi_change: number, anomaly_score: number
    }[];
}

async function executeFacet(request: OIAnomalySearchRequest[], mainRequest: OIAnomalySearchRequest) {
    const facetValues: Record<string, any> = {};
    const conn = await getOIAnomalyConnection();
    const availableFacets = mainRequest.params.facets as string[];
    const processedFacets = [];
    for (const req of request) {
        const { params } = req;
        if (typeof params.facets === 'string') {
            let query = params.facetFilters && params.facetFilters.map(f => {
                const innerk = f.map(k => {
                    const [key, value] = k.split(':');
                    return `${key} = '${value}'`;
                }).join(' OR ');
                if (innerk) {
                    return `(${innerk})`
                }
            }).join(' AND ');

            if (params.numericFilters && params.numericFilters.length > 0) {
                const numericQuery = params.numericFilters.join(' AND ');
                query = query ? `${query} AND ${numericQuery}` : numericQuery;
            }
            const result = await conn.send(`
                WITH T AS(
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
        )
                SELECT ${params.facets}, COUNT(1) cnt FROM
    T
                ${query && 'WHERE ' + query} 
                GROUP BY ${params.facets} `);
            facetValues[params.facets] = result.readAll().flatMap(k => k.toArray().map((row) => row.toJSON())).reduce((r, c) => { r[c[params.facets as string]] = c['cnt']; return r; }, {})
            processedFacets.push(params.facets);
        }
    }

    for (const facet of availableFacets) {
        if (processedFacets.includes(facet)) continue;

        let query = mainRequest.params.facetFilters && mainRequest.params.facetFilters.map(f => {
            const innerk = f.map(k => {
                const [key, value] = k.split(':');
                return `${key} = '${value}'`;
            }).join(' OR ');
            if (innerk) {
                return `(${innerk})`
            }
        }).join(' AND ');
        if (mainRequest.params.numericFilters && mainRequest.params.numericFilters.length > 0) {
            const numericQuery = mainRequest.params.numericFilters.join(' AND ');
            query = query ? `${query} AND ${numericQuery}` : numericQuery;
        }
        console.log(`executing query for facet: ${facet}: ${query} `);

        const result = await conn.send(`
            WITH T AS(
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
    )
            SELECT ${facet}, COUNT(1) cnt FROM
    T
            ${query && 'WHERE ' + query} 
            GROUP BY ${facet} `);
        facetValues[facet] = result.readAll().flatMap(k => k.toArray().map((row) => row.toJSON())).reduce((r, c) => { r[c[facet]] = c['cnt']; return r; }, {})
    }

    return facetValues;
}