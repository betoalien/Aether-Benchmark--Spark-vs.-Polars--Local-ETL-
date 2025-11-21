import os
import sys
from pyspark.sql import SparkSession
import time
import psycopg2  # Library to handle Postgres connection manually for cleanup

# --- WINDOWS PATCH CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HADOOP_HOME = os.path.join(BASE_DIR, "hadoop")
HADOOP_BIN = os.path.join(HADOOP_HOME, "bin")
# Path to the Postgres JDBC driver
JAR_PATH = os.path.join(BASE_DIR, "postgresql-42.7.2.jar")

# Inject Env Vars
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["hadoop.home.dir"] = HADOOP_HOME
os.environ["PATH"] += os.pathsep + HADOOP_BIN

# --- DATABASE CONFIGURATION ---
DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "alien666"
DB_TABLE = "sales"
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:5432/{DB_NAME}"

# Paths
PARQUET_SOURCE = "SPARK_WAREHOUSE"  # Folder created in the previous step

def clean_table():
    """
    Truncates the table before testing to ensure a clean state 
    without dropping the schema.
    """
    print(f"🧹 CLEANING TABLE '{DB_TABLE}'...")
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE {DB_TABLE};")
        conn.commit()
        cur.close()
        conn.close()
        print("✨ Table truncated successfully.")
    except Exception as e:
        print(f"⚠️ Error cleaning table: {e}")

def spark_load():
    # 1. Clean Destination
    clean_table()

    print(f"🐘 STARTING SPARK SESSION (JVM WARMUP)...")
    
    try:
        spark = SparkSession.builder \
            .appName("Spark_DB_Loader") \
            .config("spark.jars", JAR_PATH) \
            .config("spark.driver.memory", "4g") \
            .master("local[*]") \
            .getOrCreate()
    except Exception as e:
        print(f"❌ FATAL ERROR: {e}")
        return

    print("🔥 READING PARQUET AND WRITING TO POSTGRES...")
    start_time = time.time()

    try:
        # 2. Read Parquet (Very fast, just metadata)
        df = spark.read.parquet(PARQUET_SOURCE)

        # 3. Write to JDBC
        # We use "append" because we already truncated the table manually.
        # This preserves the strict data types defined in your SQL Schema.
        df.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", DB_TABLE) \
            .option("user", DB_USER) \
            .option("password", DB_PASS) \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", "10000") \
            .mode("append") \
            .save()

        duration = time.time() - start_time
        
        print("-" * 60)
        print(f"✅ SPARK LOAD COMPLETED.")
        print(f"⏱️  Execution Time: {duration:.4f} seconds")
        print("-" * 60)
        
    except Exception as e:
        print(f"💥 ERROR DURING PROCESSING: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    if not os.path.exists(JAR_PATH):
        print("❌ ERROR: postgresql-42.7.2.jar not found in current folder.")
    else:
        spark_load()