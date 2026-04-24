from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("Parquet Delete Demo")
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

print("Original count:", df.count())

# 2. Delete kyle Wiley
df_filtered = df.filter(col("Name") != "kyle Wiley")

print("After delete:", df_filtered.count())

# 3. Overwrite parquet
df_filtered.write.mode("overwrite").parquet("output/parquet/healthcare_cleaned")

print("Deleted kyle Wiley and saved to Parquet")

# 4. Simple Output Preview
print("\nPreview cleaned data (Parquet):")
df_filtered.select("Name").show(5, False)

print("Check if kyle Wiley exists:")
df_filtered.filter(df_filtered.Name == "kyle Wiley").show()

print(f"Total rows after delete: {df_filtered.count()}")

spark.stop()