from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Dow%"))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "city")
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("sqft")).otherwise(0)).alias("chiang_mai_sqft"),
        F.min("sqft").alias("min_sqft"),
    )
)
