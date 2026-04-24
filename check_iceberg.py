from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .config("spark.jars", "jars/iceberg-spark-runtime.jar")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg_warehouse")
    .master("local[*]")
    .getOrCreate()
)

spark.sql("""
SELECT * FROM local.healthcare.patients
WHERE Name = 'kyle Wiley'
""").show()