import ky from "https://esm.sh/ky@1.8.2";
const logtail_token = Deno.env.get("LOGTAIL_SOURCE_TOKEN");
const logtail_ingest_url = Deno.env.get("LOGTAIL_INGEST_URL");  // || "https://in.logs.betterstack.com/";

const _logger = () => {
    if (!logtail_token || !logtail_ingest_url) {
        return {
            info: (message: string, data: any) => console.info(message, data),
            error: (message: string, data: any) => console.error(message, data),
            warn: (message: string, data: any) => console.warn(message, data),
            debug: (message: string, data: any) => console.debug(message, data)
        };
    }

    const kyInstance = ky.extend({
        prefixUrl: logtail_ingest_url,
        headers: {
            Authorization: `Bearer ${logtail_token}`,
        }        
    });

    return {
        info: (message: string, data: any) => kyInstance.post('', { json: { message, ...data } }),
        error: (message: string, data: any) => kyInstance.post('', { json: { message, ...data } }),
        warn: (message: string, data: any) => kyInstance.post('', { json: { message, ...data } }),
        debug: (message: string, data: any) => kyInstance.post('', { json: { message, ...data } }),
    };
};



export const logger = _logger();