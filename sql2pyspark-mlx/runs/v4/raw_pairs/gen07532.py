from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 17328, "LARGE").otherwise("SMALL"))
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date"))
    .groupingSets(
        [["sqft_tier", "city", "week_start"], ["sqft_tier", "city"], ["sqft_tier"]],
        "sqft_tier",
        "city",
        "week_start",
    )
    .agg(
        F.sum(F.when(F.col("region") == "North", F.col("sqft")).otherwise(0)).alias("north_sqft"),
        F.max(F.when(F.col("region") == "West", F.col("sqft"))).alias("west_max_sqft"),
        F.percentile_approx("sqft", 0.95).alias("sqft_p95"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "East", 1)), F.lit(0)), 2).alias("sqft_per_east"),
        F.percentile_approx("sqft", 0.9).alias("sqft_p90"),
    )
    .filter(F.col("sqft_p95") > 0)
)
