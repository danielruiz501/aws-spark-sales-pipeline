from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum


# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("AWS Spark Sales Pipeline") \
    .getOrCreate()

# Leer archivo CSV
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/sales.csv")

# Mostrar estructura de los datos
print("=== ESQUEMA DE LOS DATOS ===")
df.printSchema()

# Mostrar datos originales
print("=== DATOS ORIGINALES ===")
df.show()

# Calcular ventas totales
df_transformed = df.withColumn(
    "total_sales",
    col("quantity") * col("price")
)

# Mostrar datos transformados
print("=== DATOS TRANSFORMADOS ===")
df_transformed.show()

# Guardar datos transformados en formato Parquet
df_transformed.write \
    .mode("overwrite") \
    .parquet("data/processed")

# Calcular ventas totales por categoría
sales_by_category = df_transformed.groupBy("category") \
    .agg(sum("total_sales").alias("category_sales")) \
    .orderBy(col("category_sales").desc())

print("=== VENTAS POR CATEGORÍA ===")
sales_by_category.show()

# Guardar resumen de ventas por categoría
sales_by_category.write \
    .mode("overwrite") \
    .parquet("data/processed/sales_by_category")

print("=== RESUMEN GUARDADO ===")

print("=== ETL COMPLETADO ===")
print("Datos guardados en data/processed/")

# Detener Spark
spark.stop()