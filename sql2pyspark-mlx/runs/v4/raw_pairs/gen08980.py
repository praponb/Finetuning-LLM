from pyspark.sql import functions as F

kept_keys = (
    df_stores
    .select("id", "city")
    .subtract(
        df_stores
        .filter(F.col("city") == "New York")
        .select("id", "city")
    )
)

reconciled = (
    df_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 15522, "PREMIUM").otherwise("STANDARD"))
    .withColumn("activity_month", F.date_trunc("MONTH", "opened_date"))
    .groupingSets(
        [["sqft_tier", "activity_month"], ["sqft_tier"]],
        "sqft_tier",
        "activity_month",
    )
    .agg(
        F.percentile_approx("sqft", 0.5).alias("sqft_p50"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
        F.avg("sqft").alias("avg_sqft"),
    )
    .filter(F.col("east_count") >= 1)
    .orderBy(F.col("sqft_p50").desc())
)
