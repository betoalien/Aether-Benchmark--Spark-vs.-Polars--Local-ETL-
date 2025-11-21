import os
import sys
from pyspark.sql import SparkSession
import time
import shutil

# --- WINDOWS PATCH CONFIGURATION ---
# Get the absolute path of the folder where this script is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Define where our mock hadoop folder is
HADOOP_HOME = os.path.join(BASE_DIR, "hadoop")
# The bin folder inside hadoop
HADOOP_BIN = os.path.join(HADOOP_HOME, "bin")

# 1. INJECT ENVIRONMENT VARIABLES (HACK)
# This tricks Java into thinking Hadoop is properly installed on Windows
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["hadoop.home.dir"] = HADOOP_HOME
# Add the bin to the PATH for this session only
os.environ["PATH"] += os.pathsep + HADOOP_BIN

print(f"🔧 HADOOP PATCH ACTIVATED AT: {HADOOP_HOME}")

# Data Configuration
# Assuming the folder is one level up relative to this script
INPUT_DIR = "../Ventas_Mexico" 
OUTPUT_DIR = "SPARK_WAREHOUSE"

def spark_ingest():
    # Pre-cleanup
    if os.path.exists(OUTPUT_DIR):
        try:
            shutil.rmtree(OUTPUT_DIR)
        except OSError as e:
            print(f"⚠️ Could not delete previous folder (Windows might be blocking it): {e}")

    print(f"🐘 STARTING SPARK SESSION (JVM WARMUP)...")
    
    try:
        spark = SparkSession.builder \
            .appName("Spark_Legacy_Benchmark") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.warehouse.dir", OUTPUT_DIR) \
            .master("local[*]") \
            .getOrCreate()
    except Exception as e:
        print(f"❌ FATAL ERROR STARTING JVM: {e}")
        print("Make sure you have Java 11 or 17 installed and JAVA_HOME configured.")
        return

    print("🔥 INGESTING DATA WITH PYSPARK...")
    start_time = time.time()

    try:
        # 2. Read
        # We read the folder containing CSVs
        df = spark.read.option("header", "true") \
            .option("inferSchema", "true") \
            .csv(INPUT_DIR)

        # 3. Write
        # Spark writes a folder of parquet files (partitioned or sharded)
        df.write.mode("overwrite").parquet(OUTPUT_DIR)

        duration = time.time() - start_time
        
        # Count to validate (triggers an action)
        count = df.count()

        print("-" * 60)
        print(f"✅ SPARK INGEST COMPLETED.")
        print(f"⏱️  Execution Time: {duration:.4f} seconds")
        print(f"📦 Rows Processed: {count:,}")
        print(f"📂 Location: {OUTPUT_DIR}")
        print("-" * 60)
        
    except Exception as e:
        print(f"💥 ERROR DURING PROCESSING: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    # Check for binaries before running
    if not os.path.exists(os.path.join(HADOOP_BIN, "winutils.exe")):
        print("❌ ERROR: winutils.exe is missing in hadoop/bin folder")
        print("Download it and place it there to continue.")
    else:
        spark_ingest()