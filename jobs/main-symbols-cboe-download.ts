import ky from "https://esm.sh/ky@1.2.3";
import { parse } from "jsr:@std/csv";

console.log(`Downloading symbols from cboe.`);

const symobolsText = await ky(
    "https://www.cboe.com/us/options/symboldir/equity_index_options/?download=csv",
).text();

console.log(`Parsing csv...`);

const parseData = parse(symobolsText, {
    trimLeadingSpace: true,
    columns: ["name", "symbol", "description", "temp"],
    skipFirstRow: true,
    strip: true,
});

const data = parseData.map(({ name, symbol }) => ({ name, symbol }));

Deno.writeTextFileSync(
    "./data/symbols.json",
    JSON.stringify(data, null, 4),
);

console.log(`Symbols downloaded successfully!`);