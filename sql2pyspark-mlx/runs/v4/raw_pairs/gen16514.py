from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "coupon_codes", F.explode("attributes").alias("key", "attr_value"))
)

valid = (
    lines
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.upper("key").alias("key_upper"),
        F.coalesce("key", F.lit("DIRECT")).alias("resolved_key"),
    )
)

base = (
    valid
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "*",
        F.sort_array("coupon_codes").alias("sorted_coupon_codes"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("resolved_key_2"),
    )
)

filtered = (
    base
    .filter(F.array_contains("coupon_codes", "WELCOME5"))
    .select(
        "id",
        "coupon_codes",
        "key",
        "resolved_key",
        F.length("key").alias("key_length"),
        F.coalesce("key", F.lit("NONE")).alias("key_filled"),
    )
)

result = (
    filtered
    .withColumn("key_length_tier", F.when(F.col("key_length") >= 69, "HIGH").otherwise("LOW"))
    .rollup("key_length_tier", "key")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("key_length") / F.nullif(F.count(F.when(F.col("key") == "gift", 1)), F.lit(0)), 2).alias("key_length_per_gift"),
        F.percentile_approx("key_length", [0.25, 0.5, 0.75]).alias("key_length_quartiles"),
        F.avg(F.when(F.col("key") == "gift", F.col("key_length"))).alias("gift_avg_key_length"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("key_length_per_gift") >= 0)
    .orderBy("key_length_per_gift")
)
