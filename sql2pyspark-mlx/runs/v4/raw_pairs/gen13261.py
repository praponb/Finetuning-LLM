from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "attr_value"),
    )
)

filtered = (
    lines
    .filter(F.array_contains("coupon_codes", "BUNDLE"))
    .withColumn("coupon_codes_count", F.size("coupon_codes"))
    .withColumn("k_prefix", F.substring("k", 1, 2))
    .withColumn("k_clean", F.coalesce("k", F.lit("OTHER")))
)

valid = (
    filtered
    .filter(F.col("coupon_codes_count").between(22, 43))
    .select(
        "id",
        "coupon_codes",
        "k_prefix",
        F.col("coupon_codes_count").cast("double").alias("coupon_codes_count_num"),
    )
)

cleaned = (
    valid
    .filter(F.size("coupon_codes") >= 1)
    .withColumn("coupon_codes_count_num_filled", F.coalesce("coupon_codes_count_num", F.lit(0)))
    .withColumn("is_spring10", F.array_contains("coupon_codes", "SPRING10"))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("coupon_codes_count_num_filled") >= 72, "PREMIUM").otherwise("STANDARD").alias("coupon_codes_count_num_filled_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("coupon_codes_count_num_filled").alias("total_coupon_codes_count_num_filled"),
        F.round(F.avg("coupon_codes_count_num_filled"), 2).alias("avg_coupon_codes_count_num_filled"),
        F.percentile_approx("coupon_codes_count_num_filled", 0.95).alias("coupon_codes_count_num_filled_p95"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .orderBy("coupon_codes_count_num_filled_tier")
)
