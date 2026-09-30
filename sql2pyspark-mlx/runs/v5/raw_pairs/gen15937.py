from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") != 10764)
    .groupBy(
        F.when(F.col("sqft") >= 14293, "high").when(F.col("sqft") >= 5773, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.avg("sqft").alias("avg_sqft"),
    )
    .filter(F.col("sqft_range") < 46)
    .select("sqft_bucket", "sqft_range", "avg_sqft")
)
