import { io } from "https://esm.sh/socket.io-client";
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

socket.on("worker-volatility-request", async (args) => {
    console.log("Worker volatility request received:", JSON.stringify(args));
    using stack = new DisposableStack();
    const instance = await DuckDBInstance.create(":memory:");
    stack.defer(() => instance.closeSync());
    const connection = await instance.connect();
    stack.defer(() => connection.closeSync());

    let rows = [];
    let hasError = false;
    try {
        const queryToExecute = `SELECT to_json(t)    
            FROM (
                SELECT *
                FROM '${DATA_DIR}/symbol=${args.symbol}/*.parquet'
                LIMIT 10
            ) t`;
        const result = await connection.runAndReadAll(queryToExecute)
        rows = result.getRows().map(r => JSON.parse(r[0]));
    }
    catch (err) {
        console.log(`error occurred while processing request`);
        hasError = true;
    }
    socket.emit(`worker-volatility-response`, {
        symbol: args.symbol,
        requestId: args.requestId,
        hasError,
        value: rows
    });

    console.log("Worker volatility request completed! ", JSON.stringify(args));
});

socket.on("register-worker-success", a => { console.log("worker registration succeeded", JSON.stringify(a)) })

socket.on("reconnect_attempt", (attempt) => {
  console.log(`Reconnection attempt #${attempt}`);
});

socket.on("reconnect", () => {
  console.log(`Reconnected successfully! Socket ID: ${socket.id}`);
  socket.emit("register-worker", {});
});
