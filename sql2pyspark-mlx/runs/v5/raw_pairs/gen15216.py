from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

valid = (
    lines
    .filter(F.col("created_at") >= F.lit("2025-03-01").cast("date"))
    .select(
        "id",
        "coupon_codes",
        "attr_key",
        "v",
        F.array_contains("coupon_codes", "BUNDLE").alias("is_bundle"),
        F.initcap(F.trim("attr_key")).alias("attr_key_title"),
        F.coalesce("attr_key", F.lit("NONE")).alias("resolved_attr_key"),
    )
)

base = (
    valid
    .filter(F.col("attr_key").isNotNull())
    .withColumn("clean_attr_key", F.trim("attr_key"))
)

filtered = (
    base
    .filter(~F.col("is_bundle"))
    .select(
        "*",
        F.substring("attr_key", 1, 4).alias("attr_key_prefix"),
        F.coalesce("attr_key", F.lit("NONE")).alias("resolved_attr_key_2"),
    )
)

result = (
    filtered
    .groupBy("attr_key")
    .agg(
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
