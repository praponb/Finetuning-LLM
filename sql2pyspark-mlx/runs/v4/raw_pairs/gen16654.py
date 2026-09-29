from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.length("store_name") >= 14)
    .groupBy(
        F.when(F.col("sqft") >= 13630, "high").when(F.col("sqft") >= 2562, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
    )
    .filter(F.col("chiang_mai_count") > 67)
    .orderBy("sqft_bucket", "chiang_mai_count")
    .limit(5)
)
