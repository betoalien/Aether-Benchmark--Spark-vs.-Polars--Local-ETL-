# Aether Benchmark: Spark vs. Polars (Local ETL)

**"Is Spark overkill for local data processing? Can Rust beat the JVM on a standard laptop?"**

This repository contains the source code and benchmark scripts used in the Aether Project research series. We pit Apache Spark (the industry standard) against Polars (the Rust-based challenger) to process 50 million rows of sales data on a standard Intel Core i5 laptop with 16GB RAM.

The goal? To prove that modern, vectorized engines can outperform distributed systems for "Mid-Data" workloads (1GB - 100GB) without the overhead of the JVM.

---

## 📝 Read the Full Story

Dive into the detailed analysis, methodology, and narrative behind this benchmark on my blog:

### Platform

**English Article**  
### Substack  
https://albertocardenas.substack.com/p/is-spark-overkill-how-we-cut-our

### Medium  
https://medium.com/@albertocardenascom/is-spark-overkill-how-we-cut-our-etl-time-by-70-by-switching-engines-fbd57bbd77ab?postPublishedType=initial

**Artículo en Español**
### Substack  
https://albertocardenas.substack.com/p/es-spark-overkill-como-redujimos

### Medium  
http://medium.com/@albertocardenascom/es-spark-overkill-c%C3%B3mo-redujimos-nuestros-tiempos-de-etl-en-un-70-cambiando-de-motor-163a96aa378a?postPublishedType=initial

---

## 🧪 The Benchmark Scenarios

We test two critical ETL phases:

### 1. Conversion (Chaos to Order)
Reading 50 fragmented CSV files (~5GB total) and converting them to Parquet.  
**Contenders:** pyspark vs. polars (streaming mode).

### 2. Ingestion (The Last Mile)
Loading the resulting Parquet data into a PostgreSQL database.  
**Contenders:** Spark JDBC vs. Polars + Native COPY Protocol.

---

## 🚀 Getting Started

### 1. Prerequisites

- Python 3.9+
- Java 11 or 17 (Only required for Spark)
- PostgreSQL (Running locally on port 5432)

### 2. Installation

Clone this repository and install the dependencies. A virtual environment is recommended.

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install libraries
pip install -r requirements.txt
```

### 3. The Data (Missing Folder?)

Important: The `Ventas_Mexico` folder is not included.  
The full dataset weighs 3.44 GB, exceeding GitHub's limits.

To replicate:  
Generate your own dummy dataset of 50 CSV files (1 million rows each) with columns such as:

- transaction_id  
- date  
- amount  
- category  

Place your CSVs in a folder named `Ventas_Mexico` (or update the `INPUT_DIR` variable in the scripts).

---

## ⚔️ Running the Tests

### Round 1: CSV to Parquet Conversion

Run Spark:

```bash
python spark_testing/spark_converter.py
```

*Note: The Spark script includes a mock Hadoop environment patch for Windows.*

Run Polars:

```bash
python polars_testing/polars_converter.py
```

### Round 2: Loading to PostgreSQL

Ensure your Postgres instance is running and credentials are properly configured.

Run Spark (JDBC):

```bash
python spark_testing/spark_db_loader.py
```

Run Polars (Native COPY):

```bash
python polars_testing/polars_db_loader.py
```

---

## 📊 Results Snapshot (Intel i5 / 16GB RAM)

| Task              | Engine        | Time (Seconds) | Speedup |
|------------------|--------------|----------------|---------|
| CSV -> Parquet   | Apache Spark | 336.02 s       | 1x      |
| CSV -> Parquet   | Polars       | 90.46 s        | 3.7x 🚀 |
| Load to DB       | Apache Spark | 902.33 s       | 1x      |
| Load to DB       | Polars       | 463.97 s       | 1.9x 🚀 |

---

## 👤 Author

**Alberto Cárdenas**  
Engineer, Researcher, and Creator of Aether.

Exploring the intersection of high-performance computing and developer experience.

**📧 Contact:** conect@albertocardenas.com  
**🌐 Website:** https://www.albertocardenas.com

---

## Disclaimer

This code is for educational and benchmarking purposes. It is optimized for a specific local environment and dataset structure.
