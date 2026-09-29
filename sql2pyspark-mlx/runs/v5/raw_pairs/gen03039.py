from pyspark.sql import functions as F

lines = df_carts.select("id", "coupon_codes", F.explode("attributes").alias("key", "value"))

filtered = (
    lines
    .filter(F.col("key").isNotNull() & F.array_contains("coupon_codes", "BUNDLE"))
    .withColumn("key_lower", F.lower("key"))
)

base = (
    filtered
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "coupon_codes",
        "key",
        "value",
        "key_lower",
        F.coalesce("key", F.lit("Unknown")).alias("key_filled"),
    )
)

valid = (
    base
    .filter(F.col("key").isNotNull() & (F.size("coupon_codes") >= 1))
    .select(
        "id",
        "coupon_codes",
        "key",
        "value",
        "key_filled",
        F.array_join("coupon_codes", "|").alias("coupon_codes_text"),
        F.coalesce("key", F.lit("DIRECT")).alias("key_clean"),
    )
)

result = (
    valid
    .groupBy("key")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)
