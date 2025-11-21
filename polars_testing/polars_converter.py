import os
import time
import polars as pl
import glob

# Data Configuration
# Assuming the folder is one level up relative to this script
INPUT_DIR = "../Ventas_Mexico"
# Polars usually writes a single file, but we will create a folder to match the "Warehouse" concept
OUTPUT_DIR = "POLARS_WAREHOUSE"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "data.parquet")

def polars_ingest():
    # Pre-cleanup: Create directory if it doesn't exist
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    print(f"🐻 STARTING POLARS ENGINE (RUST POWER)...")
    
    # Construct the glob pattern to find all CSVs in the folder
    # This handles cases where INPUT_DIR contains multiple CSV parts
    csv_pattern = os.path.join(INPUT_DIR, "*.csv")
    
    print("🔥 INGESTING DATA WITH POLARS...")
    start_time = time.time()

    try:
        # 1. Lazy Scan
        # scan_csv is lazy; it doesn't load data into RAM yet.
        # It creates a query plan.
        q = pl.scan_csv(csv_pattern)

        # 2. Write (Streaming)
        # sink_parquet executes the query plan and streams results to disk.
        # This allows processing datasets larger than RAM.
        q.sink_parquet(OUTPUT_FILE)

        duration = time.time() - start_time
        
        # Verification: We do a quick scan of the generated parquet to get the count
        # metadata extraction is instant in Parquet
        count = pl.scan_parquet(OUTPUT_FILE).select(pl.len()).collect().item()

        print("-" * 60)
        print(f"✅ POLARS INGEST COMPLETED.")
        print(f"⏱️  Execution Time: {duration:.4f} seconds")
        print(f"📦 Rows Processed: {count:,}")
        print(f"📂 Location: {OUTPUT_FILE}")
        print("-" * 60)

    except Exception as e:
        print(f"💥 ERROR DURING PROCESSING: {e}")

if __name__ == "__main__":
    polars_ingest()