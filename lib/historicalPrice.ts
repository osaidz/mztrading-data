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
        if (start == dayjs().format('YYYY-MM-DD')) {
            if (!isUSMarketOpenedForToday()) {
                const resp = await yf.historical(symbol, {
                    interval: '1d',
                    period1: dayjs().add(-7, 'day').toDate(),
                    period2: new Date()
                });
                return resp.pop()?.close?.toFixed(2);
            }
        }

        //TODO: need to revisit this logic as it can potentially cause issues if market is still open since there might not be any close price available

        const resp = await yf.chart(symbol, {
            interval: '1d',
            period1: dayjs(start).add(-7, 'day').toDate(),
            period2: dayjs(start).toDate()
        })
        return resp.quotes.at(-1)?.close?.toFixed(2);

        // const resp = await yf.historical(symbol, {
        //     interval: '1d',
        //     period1: dayjs(start).add(-7, 'day').toDate(),  //in case of weekend it somehow blanking out
        //     period2: dayjs(start).toDate()
        // });

        // return resp.at(-1)?.close.toFixed(2);
    } catch (error) {

    }
    return null;
}


function isUSMarketOpenedForToday(): boolean {
    const currentTime = dayjs().tz('America/New_York'); // Set timezone to Eastern Time (ET)
    const currentHour = currentTime.hour();
    const currentMinute = currentTime.minute();
    if (currentHour < 9) return false;
    if (currentHour > 9) return true;
    return (currentHour >= 9 && currentMinute >= 30);
}
