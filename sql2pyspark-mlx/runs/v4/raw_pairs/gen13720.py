from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

lines = (
    flattened
    .select("id", "attr_key", "value", F.explode_outer("coupon_codes").alias("tag"))
)

filtered = (
    lines
    .filter(F.col("tag").isNotNull())
    .select(
        "*",
        F.lower("tag").alias("tag_lower"),
        F.coalesce("tag", F.lit("N/A")).alias("tag_clean"),
    )
)

base = (
    filtered
    .filter(F.col("tag").isNotNull())
    .select(
        "id",
        "attr_key",
        "value",
        "tag",
        "tag_lower",
        "tag_clean",
        F.upper("tag").alias("tag_upper"),
        F.coalesce("attr_key", F.lit("NONE")).alias("attr_key_clean"),
    )
)

result = (
    base
    .groupBy("tag")
    .agg(
        F.count(F.when(F.col("attr_key") == "gift", 1)).alias("gift_rows"),
        F.size(F.collect_set("tag")).alias("num_tags"),
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("num_rows").desc_nulls_last())
)
