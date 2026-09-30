from pyspark.sql import functions as F

src = (
    df_stores
    .filter((F.col("city") == "Bangkok") | (F.year("opened_date") >= 2024))
    .withColumn("is_london", F.when(F.col("city") == "London", 1).otherwise(0))
)

is_london_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("is_london") >= 90, "high").when(F.col("is_london") >= 38, "mid").otherwise("low").alias("is_london_bucket"),
    )
    .agg(F.count(F.when(F.col("region") == "North", 1)).alias("north_count"))
)

result = (
    is_london_bucket_summary
    .select("is_london_bucket", "north_count")
    .orderBy(F.col("north_count").desc(), "is_london_bucket")
    .limit(5)
)
