import { getZipAssetInfoByDate, getZipAssetUrlForSymbol, zipServiceUrl } from "../lib/data.ts";
const cache = await caches.open("default");
import { Hono } from "https://esm.sh/hono@4.10.7"

const app = new Hono()

app.get('/api/snapshots', async ({ req }) => {
    const dt = req.query('dt');;
    const symbol = req.query('symbol');
    const f = req.query('f');
    if (symbol && dt && f) {
        const zipAssetUrl = getZipAssetUrlForSymbol(symbol, dt);
        const assetUrl = `${zipServiceUrl}?f=${f}&q=${zipAssetUrl}`;
        const cached = await cache.match(assetUrl);
        if (cached) {
            console.log(`Serving from cache. symbol=${symbol}, dt=${dt}, f=${f}`);
            return cached;
        }

        console.log(`Fetching from network. symbol=${symbol}, dt=${dt}, f=${f}`);
        const res = await fetch(assetUrl);
        if (!res.ok) return res;

        await cache.put(assetUrl, res.clone());
        return res;
    }
    return new Response("Use /api/snapshots?symbol=<symbol>&dt=<dt>&f=<f> to fetch snapshot files.");
});

app.delete('/api/snapshots/cache', async (c) => {
    const dt = c.req.query('dt');
    if (!dt) return new Response("Missing dt parameter", { status: 400 });
    const info = getZipAssetInfoByDate(dt);
    if (info?.zipAssetUrl) {
        const allCacheKeys = info.fileNames.map(f => `${zipServiceUrl}?f=${f}&q=${info.zipAssetUrl}`);
        const deleteCacheKeys = [];
        for (const key of allCacheKeys) {
            const cached = await cache.match(key);
            if (cached) {
                console.log(`Deleting cache for key: ${key}`);
                await cache.delete(key);
                deleteCacheKeys.push(key);
            } else {
                console.log(`No cache found for key: ${key}`);
            }
        }
        return c.json({
            message: `Cache invalidated for date: ${dt}`,
            deleted: deleteCacheKeys
        });
    }
    return c.notFound();
});

Deno.serve(app.fetch)

// Deno.serve(async (req) => {    
//     const url = new URL(req.url);
//     if (url.pathname === "/api/snapshots") {
//         }
//         return new Response("Not Found", { status: 404 });
//     }
//     return new Response("/api/snapshots?symbol=<symbol>&dt=<dt>&f=<f>");
// });