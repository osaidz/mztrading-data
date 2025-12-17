import { io } from "https://esm.sh/socket.io-client";
import { z } from "https://esm.sh/zod@4.2.1";
const socketUrl = `https://mztrading-socket.deno.dev`
const DATA_DIR = Deno.env.get("DATA_DIR") || '/data';

import { DuckDBInstance } from "npm:@duckdb/node-api";

const socket = io(socketUrl, {
    reconnectionDelayMax: 10000,
    transports: ['websocket']
});

socket.on("connect", () => {
    console.log(`Connected to the server! Socket ID: ${socket.id}`);

    socket.emit("register-worker", {});
    console.log("Client started, waiting for requests...");

});

socket.on("disconnect", () => {
    console.log("disconnection");
});

socket.on("hello", (args) => {
    console.log(`hello response: ${JSON.stringify(args)}`);
});

const BaseSchema = {
  symbol: z.string()
    .nonempty()
    .regex(/^[a-zA-Z0-9]+$/, "Symbol must be alphanumeric"),

  lookbackDays: z.number().int().positive(),

  expiration: z.string()
    .regex(/^\d{4}-\d{2}-\d{2}$/, "Expiration must be YYYY-MM-DD format"),

  requestId: z.uuid(),
};

// delta mode
const DeltaModeSchema = z.object({
  ...BaseSchema,
  mode: z.literal("delta"),
  delta: z.number().nonnegative(),
  strike: z.null().optional(),
});

// strike mode
const StrikeModeSchema = z.object({
  ...BaseSchema,
  mode: z.literal("strike"),
  strike: z.number().positive(),
  delta: z.null().optional(),
});

export const OptionsVolRequestSchema = z.discriminatedUnion("mode", [
  DeltaModeSchema,
  StrikeModeSchema,
]);

type OptionsVolRequest = z.infer<typeof OptionsVolRequestSchema>;

/*
 {"symbol":"AAPL","lookbackDays":180,"delta":25,"expiration":"2025-11-07","mode":"delta","requestId":"977c5c7b-7e96-45d1-991b-db70992d0846"}       
*/
socket.on("worker-volatility-request", async (args: OptionsVolRequest) => {
    try {

        console.log("Worker volatility request received:", JSON.stringify(args));
        using stack = new DisposableStack();
        const instance = await DuckDBInstance.create(":memory:");
        stack.defer(() => instance.closeSync());
        const connection = await instance.connect();
        stack.defer(() => connection.closeSync());
        const { symbol, lookbackDays, delta, expiration, mode, strike, requestId } = OptionsVolRequestSchema.parse(args);
        const strikeFilter = mode == 'strike' ? ` AND strike = ${strike}` : '';
        let rows = [];
        let hasError = false;
        try {

            const queryToExecute = `
            SELECT to_json(t)    
            FROM (
                WITH I AS (
                    SELECT DISTINCT dt, iv, option_type, option_symbol, expiration, strike,
                    abs(delta) AS abs_delta,
                    abs(delta) - ${delta} AS delta_diff
                    FROM '${DATA_DIR}/symbol=${symbol}/*.parquet'
                    WHERE expiration = '${expiration}' AND dt >= current_date - ${lookbackDays} ${ strikeFilter }
                ), M AS (
                    SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY dt, option_type ORDER BY delta_diff ASC) AS rn
                    FROM I
                )
                SELECT 
                    array_agg(DISTINCT dt ORDER BY dt) AS dt,
                    array_agg(iv ORDER BY dt) FILTER (WHERE option_type='C') AS cv,
                    array_agg(iv ORDER BY dt) FILTER (WHERE option_type='P') AS pv
                FROM M
                ${ mode == 'delta' ? 'WHERE rn = 1' : '' }
            ) t`;
            const result = await connection.runAndReadAll(queryToExecute)
            rows = result.getRows().map(r => JSON.parse(r[0]))[0];  //takes first row and first column
        }
        catch (err) {
            console.log(`error occurred while processing request`, err);
            hasError = true;
        }
        socket.emit(`worker-volatility-response`, {
            symbol: symbol,
            requestId: requestId,
            hasError,
            value: rows
        });

        console.log("Worker volatility request completed! ", JSON.stringify(args));

    } catch (error) {
        console.error("Error processing worker-volatility-request:", error);
    }
});

socket.on("register-worker-success", a => { console.log("worker registration succeeded", JSON.stringify(a)) })

socket.on("reconnect_attempt", (attempt) => {
    console.log(`Reconnection attempt #${attempt}`);
});

socket.on("reconnect", () => {
    console.log(`Reconnected successfully! Socket ID: ${socket.id}`);
    socket.emit("register-worker", {});
});
