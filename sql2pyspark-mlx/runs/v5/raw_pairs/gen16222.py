from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

valid = (
    lines
    .filter(F.col("key").isNotNull())
    .withColumn("key_prefix", F.substring("key", 1, 3))
    .withColumn("key_clean", F.coalesce("key", F.lit("UNKNOWN")))
)

base = (
    valid
    .filter((~F.col("key").eqNullSafe(F.lit("weight_kg"))) & (F.size("coupon_codes") > 1))
    .select(
        "*",
        F.length("key").alias("key_length"),
        F.coalesce("key", F.lit("DIRECT")).alias("key_clean_2"),
    )
)

valid_carts = (
    base
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("utm_source"))))
    .withColumn("key_length_level", F.when(F.col("key_length") >= 63, "High").when(F.col("key_length") >= 30, "Medium").otherwise("Low"))
)

result = (
    valid_carts
    .select("id", "coupon_codes", "key", "attr_value", "key_prefix")
    .orderBy(F.col("key").desc(), F.col("id").desc())
)
