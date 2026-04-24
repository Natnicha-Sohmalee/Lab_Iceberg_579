from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Check Parquet")
    .master("local[*]")
    .getOrCreate()
)

df_check = spark.read.parquet("output/parquet/healthcare_cleaned")

df_check.filter(df_check.Name == "kyle Wiley").show()

spark.stop()