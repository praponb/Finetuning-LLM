from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select("id", "customer_id", "created_at", F.col("line.qty"), F.col("line.promo"))
)

valid = (
    exploded
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & F.col("promo").isNotNull())
    .select(
        "customer_id",
        "created_at",
        "promo",
        F.lower("promo").alias("promo_lower"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("promo_filled"),
    )
)

filtered = (
    valid
    .filter(F.col("customer_id").isNotNull() & (F.col("created_at") >= "2023-05-01"))
    .withColumn("created_at_label", F.date_format("created_at", "yyyy-MM"))
    .withColumn("promo_lower_2", F.lower("promo"))
    .withColumn("resolved_promo", F.coalesce("promo", F.lit("UNKNOWN")))
)

base = (
    filtered
    .filter(F.col("customer_id").isin([1, 17, 18, 25]))
    .select(
        "customer_id",
        "created_at",
        "promo",
        "promo_lower",
        "created_at_label",
        "promo_lower_2",
        "resolved_promo",
        F.when(F.col("promo") == "VIP", 1).otherwise(0).alias("is_vip"),
    )
)

result = (
    base
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("promo") == "BUNDLE", 1)).alias("bundle_rows"),
        F.percentile_approx("is_vip", 0.95).alias("is_vip_p95"),
        F.count(F.when(F.col("promo") == "SPRING10", 1)).alias("spring10_rows"),
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.percentile_approx("is_vip", [0.25, 0.5, 0.75]).alias("is_vip_quartiles"),
        F.count(F.when(F.col("promo") == "BUNDLE", 1)).alias("bundle_count"),
    )
)
