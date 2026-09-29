from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "value"),
    )
)

valid = (
    exploded
    .filter(F.col("k").eqNullSafe(F.lit("utm_source")))
    .select(
        "*",
        F.substring("k", 1, 3).alias("k_prefix"),
        F.coalesce("k", F.lit("N/A")).alias("k_clean"),
    )
)

remaining_keys = (
    valid
    .select("id", "customer_id")
    .exceptAll(
        df_carts
        .filter(F.hour("created_at").between(10, 14))
        .select("id", "customer_id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("customer_id").isNotNull() & (F.col("k").eqNullSafe(F.lit("utm_source"))))
    .select(
        "id",
        "coupon_codes",
        "k",
        "value",
        "k_prefix",
        "k_clean",
        F.array_contains("coupon_codes", "SPRING10").alias("is_spring10"),
        F.coalesce("k", F.lit("N/A")).alias("k_filled"),
    )
)

cleaned_carts = (
    cleaned
    .filter(F.col("is_spring10"))
    .select(
        "k",
        "value",
        "k_prefix",
        "k_clean",
        "is_spring10",
        F.array_join("coupon_codes", ", ").alias("coupon_codes_text"),
    )
)

result = (
    cleaned_carts
    .groupBy("k")
    .agg(
        F.count_if(F.col("k") == "gift").alias("gift_events"),
        F.count_if(F.col("k") == "utm_source").alias("utm_source_events"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.size(F.collect_set("k")).alias("num_ks"),
        F.count(F.when(F.col("k") == "utm_source", 1)).alias("utm_source_rows"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("k").desc())
)
