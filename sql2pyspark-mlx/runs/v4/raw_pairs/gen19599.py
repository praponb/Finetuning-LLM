from pyspark.sql import functions as F

filtered_stores = df_stores.filter(F.col("opened_date") >= "2025-10-01")

stores_stats = (
    filtered_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.avg("sqft").alias("avg_sqft"))
)

result = (
    stores_stats
    .filter(F.col("avg_sqft") >= 5200)
    .select("opened_quarter", "avg_sqft")
)
