import { getOptionsSnapshotSummary, ghRepoBaseUrl } from "../../lib/data.ts";
import { format } from "https://deno.land/std@0.224.0/datetime/format.ts";

const data = getOptionsSnapshotSummary();


const releaseName = Deno.env.get("RELEASE_NAME") ||
    `DEX_GEX_SNAPSHOT_${format(new Date(), "yyyy-MM-dd")}`;
const forceDayId = Deno.env.get("FORCE_DAY_ID")

forceDayId && console.log(`Force day id for this release: ${forceDayId}`);

console.log(`🔄 Generating options snapshot for release: ${releaseName}`);

data[releaseName] = {
    displayName: forceDayId || format(new Date(), "yyyy-MM-dd"),
    created: new Date(),
    zipAssetUrl: `${ghRepoBaseUrl}/${releaseName}/options-snapshots.zip`,
    releasesBaseUrl: `https://github.com/mnsrulz/mztrading-data/releases`,
    sdResolution: "620",
    hdResolution: "1240",
    tickers: []
};

Deno.writeTextFileSync(
    "./data/options-snapshot.summary.json",
    JSON.stringify(data, null, 2),
);

console.log(`🟢 Summary file generated successfully!`);