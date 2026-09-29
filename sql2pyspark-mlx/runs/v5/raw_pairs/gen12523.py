from pyspark.sql import functions as F

lines = df_carts.select("id", F.explode("coupon_codes").alias("element"))

matched_keys = (
    lines
    .select("id")
    .exceptAll(
        df_carts
        .filter(F.array_contains("coupon_codes", "WELCOME5"))
        .select("id")
    )
)

verified = (
    lines
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter((F.col("element") == "VIP") & (F.col("element").eqNullSafe(F.lit("BUNDLE"))))
    .withColumn("is_welcome5", F.when(F.col("element") == "WELCOME5", 1).otherwise(0))
    .withColumn("element_filled", F.coalesce("element", F.lit("UNKNOWN")))
)

cleaned_carts = (
    cleaned
    .filter(~F.col("element").eqNullSafe(F.lit("BUNDLE")))
    .withColumn("is_welcome5_band", F.when(F.col("is_welcome5") >= 63, "High").when(F.col("is_welcome5") >= 20, "Medium").otherwise("Low"))
)

filtered = (
    cleaned_carts
    .filter(F.col("element").isNotNull())
    .withColumn("is_welcome5_whole", F.ceil("is_welcome5"))
)

result = (
    filtered
    .groupBy(
        "element",
        F.when(F.col("is_welcome5_whole") >= 90, "PREMIUM").otherwise("STANDARD").alias("is_welcome5_whole_tier"),
    )
    .agg(
        F.percentile_approx("is_welcome5_whole", 0.9).alias("is_welcome5_whole_p90"),
        F.percentile_approx("is_welcome5", 0.95).alias("is_welcome5_p95"),
        F.avg("is_welcome5").alias("avg_is_welcome5"),
        F.max(F.when(F.col("element") == "SPRING10", F.col("is_welcome5"))).alias("spring10_max_is_welcome5"),
        F.count(F.when(F.col("element") == "BUNDLE", 1)).alias("bundle_count"),
    )
    .orderBy("element")
)
