import ky from "https://esm.sh/ky@1.2.3";

const client = ky.create({
    headers: {
        'Accept': 'application/json'
    },
    throwHttpErrors: false,
    retry: {
        statusCodes: [429],
        limit: 3
    },
    cache: 'no-cache'
});

const kvindexmap = 'cboe-options-index-map';

const kv = await Deno.openKv();
const indexMap = new Set<string>(['SPX']);    //store the symbols which requires _ to be prefixed in the url. Add a job later on which will fetch the list of symbols from the kv store and persist in json file.
for await (const res of kv.list<string>({ prefix: [kvindexmap] })) indexMap.add(res.value);

const fetchOptionChainFromCboe = async (symbol: string) => {
    const url = `https://cdn.cboe.com/api/global/delayed_quotes/options/${indexMap.has(symbol) ? '_' : ''}${symbol}.json`;

    let response = await client(url);
    if (response.status == 403 && !indexMap.has(symbol)) {
        console.log(`403 response recieved for symbol: ${symbol}. Trying with _ prefix`);
        response = await client(`https://cdn.cboe.com/api/global/delayed_quotes/options/_${symbol}.json`);
        if (response.ok) {
            indexMap.add(symbol);
            await kv.set(['cboe-options-index-map', symbol], symbol);
        }
    }

    if (!response.ok) throw new Error('error fetching options data');

    await kv.atomic().sum([kvindexmap, symbol], 1n).commit();

    return await response.json<{
        data: {
            options: {
                option: string,
                open_interest: number,
                delta: number,
                volume: number,
                gamma: number,
            }[],
            close: number
        }
    }>();
}

export const getOptionsChain = async (symbol: string) => {
    symbol = symbol.toUpperCase();
    const optionChain = await fetchOptionChainFromCboe(symbol);
    const currentPrice = optionChain.data.close;    //TODO: is this the close price which remains same if the market is open??

    console.time(`getOptionsChain-mapping-${symbol}`)
    const mappedOptions = optionChain.data.options.map(({ option, open_interest, volume, delta, gamma }) => {
        //implement mem cache for regex match??
        const rxMatch = /(\w+)(\d{6})([CP])(\d+)/.exec(option);
        if (!rxMatch) throw new Error('error parsing option')

        return {
            strike: Number(`${rxMatch[4]}`) / 1000,
            expiration_date: `20${rxMatch[2].substring(0, 2)}-${rxMatch[2].substring(2, 4)}-${rxMatch[2].substring(4, 6)}`,
            open_interest,
            option_type: (rxMatch[3] == 'C' ? 'call' : 'put') as 'call' | 'put',
            volume,
            delta,
            gamma
        }
    });
    console.timeEnd(`getOptionsChain-mapping-${symbol}`)
    return { mappedOptions, currentPrice }
}