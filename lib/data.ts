import optionsDataSummary from "./../data/options-data.summary.json" with {
    type: "json",
};

import optionsSnapshotSummary from "./../data/options-snapshot.summary.json" with {
    type: "json",
};

type OptionsDataSummary = Record<string, {
    displayName: string;
    created: Date | string;
    symbols: Record<string, {
        fileName: string;
        assetUrl: string;
    }>;
}>;

type OptionsSnapshotSummaryFileType = {
    hdFileName: string;
    hdAssetUrl: string;
    sdFileName: string;
    sdAssetUrl: string;
};
type OptionsSnapshotSummary = Record<string, {
    displayName: string;
    created: Date | string;
    symbols: Record<string, {
        "gex": OptionsSnapshotSummaryFileType;
        "dex": OptionsSnapshotSummaryFileType;
    }>;
}>;

export const getOptionsDataSummary = () => {
    return optionsDataSummary as OptionsDataSummary;
};

export const getOptionsSnapshotSummary = () => {
    return optionsSnapshotSummary as OptionsSnapshotSummary;
};

export const OptionsSnapshotSummary= (optionsSnapshotSummary as OptionsSnapshotSummary);



export const OptionsSnapshotSummaryLegacy = Object.fromEntries(Object.keys(OptionsSnapshotSummary).map(j=> [OptionsSnapshotSummary[j].displayName, { symbols: OptionsSnapshotSummary[j].symbols} ]));

export const mapDataToLegacy = () => {
    const intermediateData = getOptionsDataSummary();
    return Object.keys(intermediateData).flatMap((j) => {
        return Object.keys(intermediateData[j].symbols).map((k) => ({
            symbol: k,
            dt: intermediateData[j].displayName,
        }));
    });
};

export const ghRepoBaseUrl = 'https://github.com/mnsrulz/mztrading-data/releases/download';
