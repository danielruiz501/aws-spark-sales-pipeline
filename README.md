# AWS Spark Sales Pipeline

**Daniel Ruiz López**  
**Junior AWS Data Engineer**

[![Python](https://img.shields.io/badge/Python-3.14-blue?logo=python&logoColor=white)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-4.2.0-orange?logo=apachespark&logoColor=white)](https://spark.apache.org/docs/latest/api/python/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.2.0-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Amazon S3](https://img.shields.io/badge/Amazon%20S3-AWS-569A31?logo=amazons3&logoColor=white)](https://aws.amazon.com/s3/)
[![Apache Parquet](https://img.shields.io/badge/Apache%20Parquet-Data%20Format-50ABF1?logo=apache&logoColor=white)](https://parquet.apache.org/)
[![Git](https://img.shields.io/badge/Git-Version%20Control-F05032?logo=git&logoColor=white)](https://git-scm.com/)
[![GitHub](https://img.shields.io/badge/GitHub-Profile-181717?logo=github&logoColor=white)](https://github.com/danielruiz501)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Profile-0A66C2?logo=linkedin&logoColor=white)](https://www.linkedin.com/in/danielruizl/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

📧 **Email:** danielruizlopez889@gmail.com

Apache Spark ETL pipeline for processing sales data stored in Amazon S3.

The project reads raw sales data from Amazon S3, performs transformations and aggregations using Apache Spark, and writes the processed results in Apache Parquet format back to Amazon S3.

## Architecture

```text
Amazon S3
   │
   │ raw/sales.csv
   ▼
Apache Spark
   │
   ├── Calculate total_amount
   │
   └── Aggregate sales by category
   │
   ▼
Amazon S3
   │
   ├── processed/sales_parquet/
   │
   └── processed/sales_by_category/

   ```

## AWS Services

- Amazon S3

## Technologies

- Python
- Apache Spark
- PySpark
- Hadoop AWS / S3A
- Amazon S3
- Parquet
- Git / GitHub

## Pipeline Process

### 1. Read raw data

The pipeline reads the sales CSV file from Amazon S3:

```text
s3a://aws-spark-sales-pipeline-daniel-2026/raw/sales.csv
```

### 2. Transform data

A new `total_amount` column is calculated:

```text
total_amount = quantity × price
```

### 3. Aggregate sales

The pipeline groups sales by category and calculates:

- Total orders
- Total units
- Total sales

### 4. Write processed data

The transformed sales dataset is stored as Parquet:

```text
processed/sales_parquet/
```

The category-level analysis is stored separately:

```text
processed/sales_by_category/

```

## Data Analysis Results

The Spark aggregation produced the following results:

| Category | Total Orders | Total Units | Total Sales |
|----------|--------------|-------------|-------------|
| Electronics | 7 | 10 | 2980 |
| Furniture | 3 | 4 | 1200 |

The processed datasets were successfully written to Amazon S3 in Apache Parquet format.

## 📸 Screenshots

### S3 Input Data

Raw sales data stored in Amazon S3 before processing.

![S3 Input Data](screenshots/s3-input.png)

### PySpark Processing

PySpark processes the sales data, calculates total sales, and performs the sales aggregation by category.

![PySpark Processing](screenshots/spark-processing.png)

### S3 Processed Data

The transformed sales dataset is stored in Amazon S3 in Apache Parquet format with Snappy compression.

![S3 Processed Data](screenshots/s3-output.png)

### Sales by Category

Aggregated sales results by category are stored separately in Amazon S3 in Apache Parquet format.

![Sales by Category](screenshots/sales-by-category.png)

## Project Structure

```text
aws-spark-sales-pipeline/
│
├── data/
│   └── sales.csv
│
├── src/
│   └── spark_etl.py
│
├── sales_pipeline.py
├── README.md
├── .gitignore
├── .gitattributes
└── LICENSE
```

## How to Run

### 1. Activate the virtual environment

Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

### 2. Run the Spark pipeline

```powershell
python sales_pipeline.py
```

The pipeline reads the raw sales data from Amazon S3, processes it with Apache Spark, and writes the results back to Amazon S3 in Apache Parquet format.




