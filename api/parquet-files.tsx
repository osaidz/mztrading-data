import { renderToString } from "npm:preact-render-to-string@^6.5.13";
import { Hono } from 'https://esm.sh/hono@4.9.8'
const app = new Hono()
import { CboeOptionsRawSummary } from '../lib/data.ts';
type OptionsSummary = { name: string, optionsAssetUrl: string, dt: string }

const optionsSummary = CboeOptionsRawSummary

app.use(async (_, next) => {
    console.log('request received', JSON.stringify(_.req));
    await next()
})

app.get('/api/cte', async (c) => {
    const tableUnion = optionsSummary.map(k => `SELECT '${k.name.match(/(\d{4}-\d{2}-\d{2})/)[1]}' AS dt, * FROM read_parquet('${k.optionsAssetUrl}')`).join(`
  UNION ALL
  `)

    const tableCte = `WITH T AS (
  ${tableUnion}
  )`

    optionsSummary.reduce((pv, cv) => {
        return `${pv}
  SELECT * FROM read_parquet'${cv.optionsAssetUrl}'`
    }, '');

    return c.json({ cte: tableCte });
})

app.get('/api/aria2', async (c) => {
    const result = optionsSummary.map(k => {
        return {
            url: `${k.optionsAssetUrl}=${k.name}.parquet`, dt: k.dt
        }
    })
    return c.json(result);
})

const App = ({ options }: { options: OptionsSummary[] }) => {
    return (
        <body>
            <div>
                <h1>Options Data</h1>
                <ul>
                    {options.map(item => (
                        <li key={item.dt}>
                            <a href={`./dt=${item.dt}/`} target="_blank">
                                dt={item.dt}
                            </a>
                        </li>
                    ))}
                </ul>
            </div>
        </body>
    );
};

const FilePage = ({ dt, fileUrl }: { dt, fileUrl: string }) => {
    return (
        <body>
            <div>
                <h1>Options Data</h1>
                <a href="./options_data.parquet" target="_blank">
                    options_data.parquet
                </a>
            </div>
        </body>
    );
}

app.get('/files', (c) => {
    return c.redirect('/files/')
})
app.get('/files/', async (c) => {

    const html = `<!DOCTYPE html>${renderToString(<App options={optionsSummary} />)}</html>`;
    return new Response(html, {
        headers: { "Content-Type": "text/html; charset=utf-8" },
    });
})
app.get('/files/:dt/options_data.parquet', (c) => {
    const dtParam = c.req.param("dt"); // "2024-12-10"  
    const dtMatch = dtParam.match(/dt=(\d{4}-\d{2}-\d{2})/);
    if (!dtMatch) return c.text("Not found", 404);
    const dt = dtMatch[1];
    const { optionsAssetUrl } = optionsSummary.find(k => k.dt === dt);
    if (optionsAssetUrl) {
        return c.redirect(optionsAssetUrl)
    } else {
        return c.text('Custom 404 Message', 404)
    }
})
app.get('/files/:dt', (c) => {
    return c.redirect(`${c.req.path}/`);
})
app.get('/files/:dt/', async (c) => {
    const dtParam = c.req.param("dt"); // "2024-12-10"  
    const dtMatch = dtParam.match(/dt=(\d{4}-\d{2}-\d{2})/);
    if (!dtMatch) return c.text("Not found", 404);
    const dt = dtMatch[1];
    const { optionsAssetUrl } = optionsSummary.find(k => k.dt === dt);
    if (optionsAssetUrl) {
        const html = `<!DOCTYPE html>${renderToString(<FilePage dt={dt} fileUrl={optionsAssetUrl} />)}</html>`;
        return new Response(html, {
            headers: { "Content-Type": "text/html; charset=utf-8" },
        });
    }

    const html = `<!DOCTYPE html><body>Not found</body></html>`;
    return new Response(html, {
        headers: { "Content-Type": "text/html; charset=utf-8" },
    });
})


Deno.serve(app.fetch)