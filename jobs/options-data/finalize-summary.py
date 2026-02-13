from pathlib import Path
import os
import pandas as pd
import json
from datetime import datetime, timezone

release_name = os.getenv("RELEASE_NAME", datetime.now().strftime("%Y-%m-%d %H:%M"))
base_path = f"temp"
stock_file = "temp/stock_data.parquet"
options_file = "temp/options_data.parquet"

root = Path(base_path)
options_files = list(root.rglob("options_data.parquet"))
stock_files = list(root.rglob("stock_data.parquet"))

if not options_files:
    print(f"No options data files found in {base_path}")
    exit(1)
if not stock_files:
    print(f"No stock data files found in {base_path}")
    exit(1)

print(f"Found {len(options_files)} options data files in {base_path}")
print(f"Found {len(stock_files)} stock data files in {base_path}")

# Read and concatenate all options data
options_dfs = []
for file in options_files:
    df = pd.read_parquet(file)
    options_dfs.append(df)
options_df = pd.concat(options_dfs, ignore_index=True)
print(f"Combined options data shape: {options_df.shape}")

# Read and concatenate all stock data
stock_dfs = []
for file in stock_files:
    df = pd.read_parquet(file)
    stock_dfs.append(df)
stock_df = pd.concat(stock_dfs, ignore_index=True)
print(f"Combined stock data shape: {stock_df.shape}")

stock_df.to_parquet(stock_file, index=False)
options_df.to_parquet(options_file, index=False)

print(f"Saved stock data to {stock_file}", flush=True)
print(f"Saved options data to {options_file}", flush=True)

# Update JSON summary file
summary_file = "data/cboe-options-summary.json"

# Load existing summary data if the file exists, otherwise start with an empty list
if os.path.exists(summary_file):
    with open(summary_file, "r") as file:
        summary_data = json.load(file)
else:
    summary_data = []

# Add a new record with the current timestamp
summary_data.append({"name": release_name, "optionsAssetUrl":f"https://github.com/mnsrulz/mztrading-data/releases/download/{release_name}/options_data.parquet", "stocksAssetUrl":f"https://github.com/mnsrulz/mztrading-data/releases/download/{release_name}/stock_data.parquet"})

# Write updated summary back to the JSON file
with open(summary_file, "w") as file:
    json.dump(summary_data, file, indent=4)

print(f"Updated summary file: {summary_file}", flush=True)