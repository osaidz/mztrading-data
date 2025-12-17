import duckdb;
import shutil, os
import json

DATA_DIR = os.environ.get("DATA_DIR")
TEMP_DIR = os.environ.get("TEMP_DIR")
MAX_DATES_LIMIT = int(os.environ.get("MAX_DATES_LIMIT", "5"))
if not DATA_DIR:
    raise ValueError(f"DATA_DIR env var is not set")
if not TEMP_DIR:
    raise ValueError(f"TEMP_DIR env var is not set")

CONFIG_FILE_NAME = "config.json"
CONFIG_FILE = os.path.join(DATA_DIR, CONFIG_FILE_NAME)

if os.path.isfile(CONFIG_FILE):
    with open(CONFIG_FILE) as f:
        configData = json.load(f)
    lastDateProcessed = configData["lastDate"]
    print(f"""Last date processed: {lastDateProcessed}""")
else:
    lastDateProcessed = "2024-01-01"    ## probably running for the first time.
    configData = {
        "lastDate" : lastDateProcessed
    }

PARQUET_SRC_DIR = os.path.join(DATA_DIR, "w2")

if not os.path.isdir(PARQUET_SRC_DIR):
     raise FileNotFoundError(f"Directory does not exist: {PARQUET_SRC_DIR}")

CONSOLIDATED_DATA_DIR = os.path.join(TEMP_DIR, "w2-output")
shutil.rmtree(CONSOLIDATED_DATA_DIR, ignore_errors=True)    # lets start fresh
os.makedirs(CONSOLIDATED_DATA_DIR, exist_ok=True)

dirs = os.listdir(PARQUET_SRC_DIR)

dt_dirs = []
for name in os.listdir(PARQUET_SRC_DIR):
    if name.startswith("dt="):
        dt_str = name.split("=")[1]
        if dt_str > lastDateProcessed:
            print(f"""{dt_str} is greater than {lastDateProcessed}""")
            dt_dirs.append(dt_str)
dt_dirs.sort()  # ensure sorting the directory
print(dt_dirs)

con = duckdb.connect()

for dt in dt_dirs[:MAX_DATES_LIMIT]:    
    dt_dir = f"dt={dt}"
    print(f"scanning {os.path.join(PARQUET_SRC_DIR, dt_dir, "*.parquet")}")
    con.execute(f"""
    COPY (
            SELECT dt, symbol, option, option_symbol, option_type,
                CAST(strike AS FLOAT)/1000 AS strike,
                CAST(strptime('20'|| expiration, '%Y%m%d') AS DATE) AS expiration,
                open_interest, volume, delta, gamma, vega, theta, rho, theo, open, high, iv, bid, ask
            FROM (
                SELECT dt,
                    REPLACE(symbol,'_', '') AS symbol,
                    option,
                    UNNEST(
                        regexp_extract(
                            option,
                            '(\\w+)(\\d{{6}})([CP])(\\d+)',
                            ['option_symbol', 'expiration', 'option_type', 'strike']
                        )
                    ),
                    open_interest, volume, delta, gamma, vega, theta, rho, theo, open, high, iv, bid, ask
                FROM read_parquet('{os.path.join(PARQUET_SRC_DIR, dt_dir, "*.parquet")}')
            ) T
        ) TO '{CONSOLIDATED_DATA_DIR}'
        (FORMAT PARQUET, PARTITION_BY (symbol), APPEND TRUE);
        --(FORMAT PARQUET, PARTITION_BY (symbol), OVERWRITE TRUE);
    """)

    configData["lastDate"] = dt

print("Processing done, dumping the config file.")
with open(os.path.join(TEMP_DIR, CONFIG_FILE_NAME), "w") as file:
    json.dump(configData, file, indent=4)