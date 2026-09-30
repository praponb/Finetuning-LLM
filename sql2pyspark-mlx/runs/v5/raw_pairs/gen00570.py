from pyspark.sql import functions as F

src = (
    df_stores
    .filter((F.col("opened_date") >= "2024-11-01") & (F.upper("store_name").like("%ALL%") | (F.col("region") == "East")))
)

sqft_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("sqft") >= 15835, "high").when(F.col("sqft") >= 5707, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.max("sqft").alias("max_sqft"),
    )
)

result = (
    sqft_bucket_summary
    .select("sqft_bucket", "max_sqft")
    .orderBy("sqft_bucket")
)
