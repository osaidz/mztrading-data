import { renderToString } from "npm:preact-render-to-string@^6.5.13";
import { h } from "npm:preact";
import { Hono } from "https://esm.sh/hono@4.9.8";
import { CboeOptionsRawSummary } from "../lib/data.ts";

type OptionsSummary = { name: string; optionsAssetUrl: string; dt: string };

const app = new Hono();
const optionsSummary: OptionsSummary[] = CboeOptionsRawSummary;

// --- Layout wrapper ---
const Html = ({ children }: { children: preact.ComponentChildren }) => (
  <html lang="en">
    <head>
      <meta charSet="UTF-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <title>Options Data</title>
    </head>
    <body>{children}</body>
  </html>
);

// --- Pages ---
const App = ({ options }: { options: OptionsSummary[] }) => (
  <div>
    <h1>Options Data</h1>
    <ul>
      {options.map((item) => (
        <li key={item.dt}>
          <a href={`./dt=${item.dt}/`} target="_blank">
            dt={item.dt}
          </a>
        </li>
      ))}
    </ul>
  </div>
);

const FilePage = ({ dt, fileUrl }: { dt: string; fileUrl: string }) => (
  <div>
    <h1>Options Data for {dt}</h1>
    <a href="./options_data.parquet" target="_blank">
      Download parquet
    </a>
  </div>
);

// --- Middlewares ---
app.use(async (c, next) => {
  console.log("request received", c.req.path);
  await next();
});

// --- API routes ---
app.get("/api/cte", async (c) => {
  const tableUnion = optionsSummary
    .map(
      (k) =>
        `SELECT '${k.name.match(/(\\d{4}-\\d{2}-\\d{2})/)[1]}' AS dt, * FROM read_parquet('${k.optionsAssetUrl}')`
    )
    .join("\n  UNION ALL\n  ");

  const tableCte = `WITH T AS (\n  ${tableUnion}\n)`;
  return c.json({ cte: tableCte });
});

app.get("/api/aria2", async (c) => {
  const result = optionsSummary.map((k) => ({
    url: `${k.optionsAssetUrl}=${k.name}.parquet`,
    dt: k.dt,
  }));
  return c.json(result);
});

// --- HTML routes ---
app.get("/files", (c) => c.redirect("/files/"));

app.get("/files/", (c) => {
  const html =
    "<!DOCTYPE html>" + renderToString(<Html><App options={optionsSummary} /></Html>);
  return c.html(html);
});

app.get("/files/:dt/options_data.parquet", (c) => {
  const dtParam = c.req.param("dt");
  const dtMatch = dtParam.match(/dt=(\d{4}-\d{2}-\d{2})/);
  if (!dtMatch) return c.text("Not found", 404);

  const dt = dtMatch[1];
  const match = optionsSummary.find((k) => k.dt === dt);

  return match ? c.redirect(match.optionsAssetUrl) : c.text("Custom 404 Message", 404);
});

app.get("/files/:dt", (c) => c.redirect(`${c.req.path}/`));

app.get("/files/:dt/", (c) => {
  const dtParam = c.req.param("dt");
  const dtMatch = dtParam.match(/dt=(\d{4}-\d{2}-\d{2})/);
  if (!dtMatch) return c.text("Not found", 404);

  const dt = dtMatch[1];
  const match = optionsSummary.find((k) => k.dt === dt);

  if (match) {
    const html =
      "<!DOCTYPE html>" +
      renderToString(<Html><FilePage dt={dt} fileUrl={match.optionsAssetUrl} /></Html>);
    return c.html(html);
  }

  const notFoundHtml = "<!DOCTYPE html>" + renderToString(<Html><body>Not found</body></Html>);
  return c.html(notFoundHtml, 404);
});

// --- Start server ---
Deno.serve(app.fetch);
