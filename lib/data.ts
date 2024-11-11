import optionsDataSummary from "./../data/options-data.summary.json" with {
    type: "json",
};

type OptionsDataSummary = Record<string, {
    displayName: string;
    created: Date;
    symbols: Record<string, {
        fileName: string;
        assetUrl: string;
    }>;
}>;

export const getOptionsDataSummary = () => {
    return optionsDataSummary as OptionsDataSummary;
};

export const mapDataToLegacy = () => {
    const intermediateData = getOptionsDataSummary();
    return Object.keys(intermediateData).flatMap((j) => {
        return Object.keys(intermediateData[j].symbols).map((k) => ({
            symbol: k,
            dt: intermediateData[j].displayName,
        }));
    });
};

