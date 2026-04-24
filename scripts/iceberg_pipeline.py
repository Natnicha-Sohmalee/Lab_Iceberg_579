from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Iceberg Delete Demo")
    .config("spark.jars", "jars/iceberg-spark-runtime.jar")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg_warehouse")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 1. Read CSV
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/healthcare_dataset.csv")
)

# 2. Reset table
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.healthcare")
spark.sql("DROP TABLE IF EXISTS local.healthcare.patients")

# 3. Create table
df.writeTo("local.healthcare.patients").create()
print("Table created")

# 4. Delete (แนะนำให้ใช้ lower กัน case พัง)
spark.sql("""
    DELETE FROM local.healthcare.patients
    WHERE lower(Name) = 'kyle wiley'
""")

print("Deleted Kyle Wiley from Iceberg")

# 5. Check
spark.sql("""
    SELECT Name FROM local.healthcare.patients
    WHERE lower(Name) = 'kyle wiley'
""").show()

# 6. Snapshot
spark.sql("""
    SELECT snapshot_id, operation
    FROM local.healthcare.patients.snapshots
""").show(truncate=False)

# 7. Preview
print("\nPreview Iceberg table:")
spark.sql("SELECT Name FROM local.healthcare.patients LIMIT 5").show()

print(f"Total rows: {spark.table('local.healthcare.patients').count()}")

spark.stop()