from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ----------------------------
# Create Spark Session
# ----------------------------
spark = (
    SparkSession.builder
    .appName("AWS Spark Sales Pipeline")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.5.0"
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .getOrCreate()
)

# Reduce Spark logs
spark.sparkContext.setLogLevel("ERROR")

# ----------------------------
# S3 paths
# ----------------------------
input_path = (
    "s3a://aws-spark-sales-pipeline-daniel-2026/"
    "raw/sales.csv"
)

output_path = (
    "s3a://aws-spark-sales-pipeline-daniel-2026/"
    "processed/sales_parquet"
)

# ----------------------------
# Read CSV from S3
# ----------------------------
print("\nReading sales data from S3...")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

# ----------------------------
# Transform data
# ----------------------------
print("\nTransforming data...")

sales_df = df.withColumn(
    "total_amount",
    col("quantity") * col("price")
)

# ----------------------------
# Show transformed data
# ----------------------------
print("\nTransformed Sales Dataset")
sales_df.show()

print("\nSchema")
sales_df.printSchema()

# ----------------------------
# Sales analysis by category
# ----------------------------
print("\nSales Analysis by Category...")

category_df = (
    sales_df
    .groupBy("category")
    .agg(
        {"order_id": "count",
         "quantity": "sum",
         "total_amount": "sum"}
    )
    .withColumnRenamed("count(order_id)", "total_orders")
    .withColumnRenamed("sum(quantity)", "total_units")
    .withColumnRenamed("sum(total_amount)", "total_sales")
)

print("\nSales by Category")
category_df.show()

# ----------------------------
# Write Parquet to S3
# ----------------------------
print("\nWriting Parquet data to S3...")

sales_df.write \
    .mode("overwrite") \
    .parquet(output_path)

# ----------------------------
# Write category analysis to S3
# ----------------------------
category_output_path = (
    "s3a://aws-spark-sales-pipeline-daniel-2026/"
    "processed/sales_by_category"
)

category_df.write \
    .mode("overwrite") \
    .parquet(category_output_path)

print(
    f"\nCategory analysis written to: "
    f"{category_output_path}"
)

print("\nPipeline completed successfully!")
print(f"Processed data written to: {output_path}")

# ----------------------------
# Stop Spark
# ----------------------------
spark.stop()