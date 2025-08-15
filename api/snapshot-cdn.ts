// import {
//     Application,
//     isHttpError,
//     Router,
// } from "https://deno.land/x/oak@v12.6.1/mod.ts";
// import { getQuery } from "https://deno.land/x/oak@v12.6.1/helpers.ts";
// import { logger } from '../lib/logger.ts'
import { getZipAssetUrlForSymbol, zipServiceUrl } from "../lib/data.ts";

// const router = new Router();
const cache = await caches.open("default");

Deno.serve(async (req) => {
    //check if the url is api/snapshots and parse the query params
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
                return cached;
            }
            const res = await fetch(assetUrl);
            if (!res.ok) return res;

            await cache.put(assetUrl, res.clone());
            return res;
        }
    }
    return new Response("Not Found", { status: 404 });
});

// router.get("/api/snapshots", async (context) => {
//     const { dt, symbol, f } = getQuery(context);
//     const zipAssetUrl = getZipAssetUrlForSymbol(symbol, dt);
//     const assetUrl = `${zipServiceUrl}?f=${f}&q=${zipAssetUrl}`;

//     const cached = await cache.match(assetUrl);
//     if (cached) {
//         context.response.body  = cached.body;
//         context.response.headers = cached.headers;

//         // context.response.headers.set('Content-Type', 'application/octet-stream');
//         // context.response.headers.set('Content-Disposition', `attachment; filename=${f}`);
//         // context.response.headers.set('Content-Length', cached);
//         // context.response.body = ReadableStream.from(file.stream());
//     }

// })

// const app = new Application();

// app.use(async (context, next) => {
//     try {
//         const req = context.request;
//         logger.info(`${req.method} ${req.url.pathname}`, {
//             path: req.url.pathname,
//             method: req.method,
//             referer: req.headers.get('referer'),
//             auth: req.headers.get('authorization'),
//             ip: req.headers.get('X-Forwarded-For') || req.headers.get('x-real-ip'),
//             userAgent: req.headers.get('user-agent'),
//             service: "mztrading-data"
//         });
//         context.response.headers.set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
//         context.response.headers.set("Access-Control-Allow-Origin", "*");
//         context.response.headers.set("Access-Control-Max-Age", "86400");
//         context.response.headers.set("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
//         await next();
//     } catch (err) {
//         if (isHttpError(err)) {
//             context.response.status = err.status;
//         } else {
//             context.response.status = 500;
//         }
//         context.response.body = { error: err.message };
//         context.response.type = "json";
//     }
// });

// app.use(router.routes());
// app.use(router.allowedMethods());
// await app.listen({ port: 8000 });
