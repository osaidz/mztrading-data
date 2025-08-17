import { getZipAssetUrlForSymbol, zipServiceUrl } from "../lib/data.ts";
const cache = await caches.open("default");

Deno.serve(async (req) => {    
    const url = new URL(req.url);
    if (url.pathname === "/api/snapshots") {
        const dt = url.searchParams.get("dt");
        const symbol = url.searchParams.get("symbol");
        const f = url.searchParams.get("f");
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
        return new Response("Not Found", { status: 404 });
    }
    return new Response("/api/snapshots?symbol=<symbol>&dt=<dt>&f=<f>");
});