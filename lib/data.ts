import optionsDataSummary from "./../data/options-data.summary.json" with {
    type: "json",
};

type OptionsDataSummary = Record<string, {
    displayName: string,
    created: Date;
    symbols: Record<string, {
        fileName: string;
        assetUrl: string;
    }>;
}>;

export const getOptionsDataSummary = () => {
    return optionsDataSummary as OptionsDataSummary;
};

