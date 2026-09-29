from pyspark.sql import functions as F

valid_stores = df_stores.filter(~((F.col("region") == "East") & (F.col("city") != "Singapore")))

city_summary = (
    valid_stores
    .groupBy("city")
    .agg(F.count("*").alias("total_count"))
)

result = (
    city_summary
    .filter(F.col("total_count") > 8)
    .select("city", "total_count")
)
