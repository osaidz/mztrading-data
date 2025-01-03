import dayjs from "https://esm.sh/dayjs@1.11.13";
import utc from "https://esm.sh/dayjs@1.11.13/plugin/utc";
import isToday from "https://esm.sh/dayjs@1.11.13/plugin/isToday";
import timezone from "https://esm.sh/dayjs@1.11.13/plugin/timezone";

import yf from 'npm:yahoo-finance2@2.11.3';

import "https://esm.sh/dayjs@1.11.13/locale/en";

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(isToday);

export const getPriceAtDate = async (symbol: string, dt: string, keepOriginalValue: boolean = false) => {
    try {
        const start = dayjs(dt.substring(0, 10)).format('YYYY-MM-DD');
        const resp = await yf.chart(symbol, {
            interval: '1d',
            period1: dayjs(start).add(-7, 'day').toDate(),
            period2: dayjs(start).toDate()
        })
        return keepOriginalValue ? resp.quotes.at(-1)?.close : resp.quotes.at(-1)?.close?.toFixed(2);
    } catch (error) {
        return null;
    }    
}
