from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "v"),
    )
)

lines = (
    exploded
    .select(
        "id",
        "created_at",
        "key",
        "v",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

base = (
    lines
    .filter((F.col("created_at") > F.lit("2025-03-01 11:00:00").cast("timestamp")) & F.col("key").isNotNull())
    .select("id", "key", F.substring("tag", 1, 2).alias("tag_prefix"))
)

valid = (
    base
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("utm_source"))))
    .select(
        "*",
        F.substring("key", 1, 4).alias("key_prefix"),
        F.when(F.col("key").isin(["device", "weight_kg"]), "Core").otherwise("Other").alias("key_group"),
    )
)

valid_carts = (
    valid
    .filter(F.col("key").isNull())
    .select(
        "id",
        "key",
        "key_prefix",
        F.length("key").alias("key_length"),
        F.coalesce("key", F.lit("N/A")).alias("resolved_key"),
    )
)

scored = (
    valid_carts
    .select(
        "id",
        "key",
        "key_prefix",
        "resolved_key",
        F.min("key_length").over(w).alias("key_min_key_length"),
    )
)

totals = (
    scored
    .withColumn("key_min_key_length_tier", F.when(F.col("key_min_key_length") >= 85, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["key", "key_min_key_length_tier"], ["key"]],
        "key",
        "key_min_key_length_tier",
    )
    .agg(
        F.avg(F.when(F.col("key") == "utm_source", F.col("key_min_key_length"))).alias("utm_source_avg_key_min_key_length"),
        F.count(F.when(F.col("key") == "device", 1)).alias("device_count"),
        F.countDistinct("key").alias("distinct_keys"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = totals.select("key", "key_min_key_length_tier", "device_count", "grouping_id")
