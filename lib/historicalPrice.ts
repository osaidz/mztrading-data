import dayjs from "https://esm.sh/dayjs@1.11.13";
import utc from "https://esm.sh/dayjs@1.11.13/plugin/utc";
import isToday from "https://esm.sh/dayjs@1.11.13/plugin/isToday";
import timezone from "https://esm.sh/dayjs@1.11.13/plugin/timezone";

import yf from 'npm:yahoo-finance2@2.11.3';

import "https://esm.sh/dayjs@1.11.13/locale/en";

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(isToday);

const EXCEPTION_SYMBOLS = {
    'SPX': '^SPX',
    'VIX': '^VIX',
} as Record<string, string>

export const getPriceAtDate = async (symbol: string, dt: string, keepOriginalValue: boolean = false) => {
    try {
        const start = dayjs(dt.substring(0, 10)).format('YYYY-MM-DD');
        const resp = await yf.chart(EXCEPTION_SYMBOLS[symbol.toUpperCase()] || symbol, {
            interval: '1d',
            period1: dayjs(start).add(-7, 'day').toDate(),
            period2: dayjs(start).toDate()
        })
        return keepOriginalValue ? resp.quotes.at(-1)?.close : resp.quotes.at(-1)?.close?.toFixed(2);
    } catch (error) {
        return null;
    }
}

export const getLastNPrices = async (symbol: string, lastN: number, interval: 'd' | 'h') => {
    const t = Math.ceil(interval == 'h' ? Math.ceil((lastN * 1.2) / 40) : Math.ceil((lastN * 1.2) / 5));       //take extra couple days of data just to be sure we have enough data
    const start = dayjs().format('YYYY-MM-DD');
    const resp = await yf.chart(EXCEPTION_SYMBOLS[symbol.toUpperCase()] || symbol, {
        interval: interval == 'd' ? '1d' : '1h',
        period1: dayjs(start).add(-t, 'week').toDate(),
        period2: dayjs(start).toDate()
    })
    return resp.quotes.map(j => j.close).filter(k => k != null).slice(-lastN);
}