from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

lines = (
    exploded
    .select(
        "id",
        "created_at",
        "attr_key",
        "attr_value",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

valid = (
    lines
    .filter(F.col("code") == "SPRING10")
    .select(
        "*",
        F.upper("attr_key").alias("attr_key_upper"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_filled"),
    )
)

valid_carts = (
    valid
    .filter((F.col("created_at") >= F.lit("2025-03-02 06:00:00").cast("timestamp")) & F.col("code").isNotNull())
    .select(
        "id",
        "attr_key",
        "attr_value",
        "code",
        "attr_key_upper",
        "attr_key_filled",
        F.coalesce("code", F.lit("Unknown")).alias("code_filled"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_filled_2"),
    )
)

summary = (
    valid_carts
    .groupBy("attr_key")
    .agg(
        F.count_if(F.col("attr_key") == "weight_kg").alias("weight_kg_events"),
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("attr_key") == "gift", 1)).alias("gift_count"),
        F.sort_array(F.collect_set("code")).alias("code_list"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    summary
    .select("attr_key", "weight_kg_events", "distinct_attr_keys", "gift_count")
    .orderBy(F.col("weight_kg_events").desc())
)
