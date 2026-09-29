from pyspark.sql import functions as F

recent_stores = df_stores.filter(F.col("opened_date").between("2023-09-17", "2025-02-01"))

stores_stats = (
    recent_stores
    .groupBy("city")
    .agg(
        F.count("*").alias("total_count"),
        F.avg("sqft").alias("avg_sqft"),
    )
)

result = (
    stores_stats
    .select("city", "total_count")
    .orderBy("city", F.col("total_count").desc())
)
