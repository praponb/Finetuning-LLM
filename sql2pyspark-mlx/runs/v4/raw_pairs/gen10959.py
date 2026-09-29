from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("rating_tier", F.when(F.col("rating") >= 4.3, "PREMIUM").otherwise("STANDARD"))
    .groupingSets([["rating_tier", "city"], ["rating_tier"], []], "rating_tier", "city")
    .agg(
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("rating_per_singapore"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("rating_per_sg"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
    )
    .orderBy(F.col("rating_per_sg").desc())
)
