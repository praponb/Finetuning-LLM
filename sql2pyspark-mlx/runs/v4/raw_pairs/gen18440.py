from pyspark.sql import functions as F

src = (
    df_stores
    .filter((F.col("region") == "Central") & F.col("sqft").between(7159, 12969) & (F.col("sqft") < 9104))
)

summary = (
    src
    .groupBy(
        F.when(F.col("sqft") >= 13005, "high").when(F.col("sqft") >= 6743, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("sqft")).otherwise(0)).alias("tokyo_sqft"),
    )
)

result = (
    summary
    .filter(F.col("max_sqft") < 5500)
    .select("sqft_bucket", "max_sqft", "tokyo_sqft")
    .orderBy("sqft_bucket")
)
