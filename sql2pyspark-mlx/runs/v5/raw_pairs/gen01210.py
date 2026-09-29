from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") <= 10472) | (F.lower(F.trim("store_name")) == "siam flagship"))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("sqft")).otherwise(0)).alias("chiang_mai_sqft"),
    )
    .filter(F.col("chiang_mai_sqft") < 47)
)
