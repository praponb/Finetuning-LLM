from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "region",
        F.when(F.col("sqft") >= 16507, "high").when(F.col("sqft") >= 8000, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.count("*").alias("store_count"),
        F.sum("sqft").alias("total_sqft"),
    )
    .filter(F.col("total_sqft") >= 58000)
    .drop("total_sqft")
)
