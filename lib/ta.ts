import { getLastNPrices } from "./historicalPrice.ts";

type indicatorType = 'emaDaily' | 'emaHourly'
type indicators = 'ema21d' | 'ema9d' | 'ema200d'
type indicatorMapType = Record<indicators, { type: indicatorType, interval: number }>
const indicatorMap: indicatorMapType = {
    'ema9d': {
        type: 'emaDaily',
        interval: 9
    },
    'ema21d': {
        type: 'emaDaily',
        interval: 21
    },
    'ema200d': {
        type: 'emaDaily',
        interval: 200
    }
}

const calculateEma = (closingPrices: number[], period: number) => {
    const k = 2 / (period + 1);
    let ema = closingPrices[0];
    for (let i = 1; i < closingPrices.length; i++) {
        ema = (closingPrices[i] * k) + (ema * (1 - k));
    }
    return ema;
}

export const getHourlyEma = async (symbol: string, intervals: number[]) => {
    if (intervals.length <= 0) return {};
    const maxInterval = Math.max(...intervals);
    const prices = await getLastNPrices(symbol, maxInterval, 'h');
    return intervals.reduce((p, i) => {
        p[`ema${i}h`] = calculateEma(prices, i)
        return p;
    }, {} as Record<string, number>)
}

export const getDailyEma = async (symbol: string, intervals: number[]) => {
    if (intervals.length <= 0) return {};
    const maxInterval = Math.max(...intervals);
    const prices = await getLastNPrices(symbol, maxInterval, 'd');
    return intervals.reduce((p, i) => {
        p[`ema${i}d`] = calculateEma(prices, i);
        return p
    }, {} as Record<string, number>)
}

export const getIndicatorValues = async (symbol: string, i: string[]) => {
    const dailyEmas = i.reduce((p, c) => {
        const valid = indicatorMap[c as indicators];
        if (valid && valid.type == 'emaDaily') {
            p.push(valid.interval)
        }
        return p;
    }, [] as number[])

    const hourlyEmas = i.reduce((p, c) => {
        const valid = indicatorMap[c as indicators];
        if (valid && valid.type == 'emaHourly') {
            p.push(valid.interval)
        }
        return p;
    }, [] as number[])

    const dailyEmaIndicators = await getDailyEma(symbol, dailyEmas);
    const hourlyEmaIndicators = await getHourlyEma(symbol, hourlyEmas);

    return { ...dailyEmaIndicators, ...hourlyEmaIndicators };
}