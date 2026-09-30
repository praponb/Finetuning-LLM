from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") > "2025-08-01")
    .groupBy("city")
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.sum(F.when(F.col("city") == "New York", 1).otherwise(0)).alias("new_york_count"),
    )
    .orderBy("avg_sqft", "city")
    .limit(10)
)
