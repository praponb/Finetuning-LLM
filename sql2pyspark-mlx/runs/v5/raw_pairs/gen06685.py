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

joined = (
    lines
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.is_active"))
    .select(
        "e.customer_id",
        "e.coupon_codes",
        "e.k",
        "e.attr_value",
        F.col("d.segment"),
        F.col("d.city"),
        F.col("d.age"),
    )
)

filtered = (
    joined
    .filter(F.col("city") == "London")
    .select(
        "coupon_codes",
        "segment",
        "age",
        F.when(F.col("k").isin(["device", "utm_source"]), "Priority").otherwise("Other").alias("k_group"),
    )
)

filtered_carts = (
    filtered
    .filter(F.col("segment").isNotNull() & F.array_contains("coupon_codes", "VIP"))
    .select(
        "*",
        F.array_join("coupon_codes", ", ").alias("coupon_codes_text"),
        F.coalesce("segment", F.lit("DIRECT")).alias("segment_filled"),
    )
)

cleaned = (
    filtered_carts
    .filter(~F.col("segment").eqNullSafe(F.lit("Consumer")))
    .withColumn("segment_upper", F.upper("segment"))
)

result = (
    cleaned
    .groupBy("segment")
    .agg(
        F.sum(F.when(F.col("segment") == "SMB", F.col("age")).otherwise(0)).alias("smb_age"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.sum(F.when(F.col("segment") == "SMB", F.col("age")).otherwise(0)).alias("smb_age_2"),
    )
    .orderBy(F.col("smb_age").desc_nulls_last())
)
