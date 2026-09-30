from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

matched_keys = (
    flattened
    .select("id")
    .subtract(
        df_carts
        .filter(F.col("created_at") >= "2023-09-01")
        .select("id")
    )
)

reconciled = (
    flattened
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-01"))) & (~F.col("k").eqNullSafe(F.lit("utm_source"))))
    .select("id", "coupon_codes", "k", "v", F.lower("k").alias("k_lower"))
)

cleaned = (
    base
    .filter(F.array_contains("coupon_codes", "VIP"))
    .withColumn("coupon_codes_text", F.array_join("coupon_codes", ", "))
    .withColumn("coupon_codes_text_2", F.array_join("coupon_codes", "|"))
    .withColumn("k_filled", F.coalesce("k", F.lit("N/A")))
)

remaining_keys = (
    cleaned
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.col("customer_id").isin([10, 14, 20]))
        .select("id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("k")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids_3"),
    )
)
