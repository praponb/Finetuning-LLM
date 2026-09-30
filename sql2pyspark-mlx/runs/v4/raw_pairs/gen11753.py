from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date"))
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 13340, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["week_start", "sqft_tier", "city"], ["week_start", "sqft_tier"], ["week_start"], []],
        "week_start",
        "sqft_tier",
        "city",
    )
    .agg(
        F.max(F.when(F.col("region") == "South", F.col("sqft"))).alias("south_max_sqft"),
        F.sum(F.when(F.col("region") == "South", F.col("sqft")).otherwise(0)).alias("south_sqft"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
        F.percentile_approx("sqft", 0.95).alias("sqft_p95"),
        F.max("sqft").alias("max_sqft"),
        F.sum(F.when(F.col("region") == "South", F.col("sqft")).otherwise(0)).alias("south_sqft_2"),
    )
    .orderBy(F.col("south_max_sqft").desc())
)
