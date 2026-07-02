import pandas as pd
import glob
import os

raw_files = glob.glob(os.path.join("raw", "*.csv"))
print(f"Found {len(raw_files)} files: {[os.path.basename(f) for f in raw_files]}")

df = pd.concat(
    [pd.read_csv(f, low_memory=False) for f in raw_files],
    ignore_index=True
)

before = len(df)
df.drop_duplicates(inplace=True)
print(f"Rows before dedup: {before}, after: {len(df)}")

df.to_csv("merged.csv", index=False)
print(f"Saved merged.csv ({len(df)} rows, {len(df.columns)} columns)")
