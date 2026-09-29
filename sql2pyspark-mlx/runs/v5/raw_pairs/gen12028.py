from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Berlin", "New York"]) & (F.col("city") == "Singapore") & F.col("store_name").isNotNull())
    .agg(F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"))
)
