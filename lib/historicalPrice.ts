import dayjs from 'https://esm.sh/dayjs';
import utc from 'https://esm.sh/dayjs/plugin/utc';
import isToday from 'https://esm.sh/dayjs/plugin/isToday';
import timezone from 'https://esm.sh/dayjs/plugin/timezone';

import yf from 'npm:yahoo-finance2@2.11.3';

import 'https://esm.sh/dayjs/locale/en';

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(isToday);

export const getPriceAtDate = async (symbol: string, dt: string) => {
    try {
        const start = dayjs(dt.substring(0, 10)).format('YYYY-MM-DD');
        const resp = await yf.chart(symbol, {
            interval: '1d',
            period1: dayjs(start).add(-7, 'day').toDate(),
            period2: dayjs(start).toDate()
        })
        return resp.quotes.at(-1)?.close?.toFixed(2);
    } catch (error) {
        return null;
    }    
}
