from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 12926, "high").when(F.col("sqft") >= 6353, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.sum(F.when(F.col("region") == "West", 1).otherwise(0)).alias("west_count"))
    .filter(F.col("west_count") < 58)
    .orderBy("sqft_bucket", F.col("west_count").desc())
)
