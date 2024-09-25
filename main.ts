import { Application, Router, isHttpError } from "https://deno.land/x/oak@v12.6.1/mod.ts";
import { sortBy } from "https://deno.land/std@0.224.0/collections/sort_by.ts";
import { getQuery } from "https://deno.land/x/oak@v12.6.1/helpers.ts";
import ky from 'https://esm.sh/ky@1.2.3';
import yf from 'npm:yahoo-finance2';
import dayjs from "https://cdn.skypack.dev/dayjs@1.10.4";

const token = Deno.env.get("ghtoken");
const router = new Router();

router.get("/", async (context) => {
  context.response.body = 'hello';
})
  .get("/summary", async (context) => {
    const { s } = getQuery(context);
    const data = await ky(`https://raw.githubusercontent.com/mnsrulz/mytradingview-data/main/summary/data.json`, {
      headers: {
        Authorization: `Bearer ${token}`
      }
    }).json();
    const filteredData = s ? data.filter(j => j.symbol.toUpperCase() == s.toUpperCase()) : data;

    const sortedByDates = sortBy(filteredData, (it) => it.dt, { order: "desc" });

    context.response.body = sortedByDates;
    context.response.type = "application/json";
  })
  .get("/data", async (context) => {
    const { dt, s } = getQuery(context);
    if (!dt || !s) throw new Error(`empty query provided. Use with ?dt=YOUR_QUERY&s=aapl`);
    // const cu = await yf.historical(s, {
    //     period1: dayjs(dt.substr(0, 10)).toDate(),
    //     interval: '1d',
    //     period2: dayjs(dt.substr(0, 10)).add(1, 'days').toDate()
    // })
    // const currentPrice = cu.at(0)?.open;
    const data = await ky(`https://raw.githubusercontent.com/mnsrulz/mytradingview-data/main/data/dt=${dt}/symbol=${s.toUpperCase()}/data.json`, {
      headers: {
        Authorization: `Bearer ${token}`
      }
    }).json();
    context.response.body = { data };
    context.response.type = "application/json";
  })
  .get("/images", async (context) => {
    console.log(`getting image`)
    const { dt, s } = getQuery(context);
    if (!dt || !s) throw new Error(`empty query provided. Use with ?dt=YOUR_QUERY&s=aapl`);
    console.log(`calling endpoint: https://api.github.com/repos/mnsrulz/mytradingview-data/releases/tags/${dt.substr(0, 10)}`)
    const { assets } = await ky(`https://api.github.com/repos/mnsrulz/mytradingview-data/releases/tags/${dt.substr(0, 10)}`, {
      headers: {
        Authorization: `Bearer ${token}`
      }
    }).json<{ assets: { url: string, name: string }[] }>();
    console.log(`found assets`);
    const { url } = assets.filter(j => j.name == `${s.toUpperCase}.png`)
    console.log(`found url: ${url}`);
    const data = await ky(url, {
      headers: {
        Authorization: `Bearer ${token}`
      }
    }).blob()
    context.response.body = data;
  });

const app = new Application();

app.use(async (context, next) => {
  try {
    context.response.headers.set('Access-Control-Allow-Origin', '*')
    await next();
  } catch (err) {
    if (isHttpError(err)) {
      context.response.status = err.status;
    } else {
      context.response.status = 500;
    }
    context.response.body = { error: err.message };
    context.response.type = "json";
  }
});


app.use(router.routes());
app.use(router.allowedMethods());
await app.listen({ port: 8000 });