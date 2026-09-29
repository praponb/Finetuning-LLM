from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 9279)
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("sqft")).otherwise(0)).alias("new_york_sqft"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
    )
    .filter(F.col("new_york_sqft") < 32)
)
