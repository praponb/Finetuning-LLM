from pyspark.sql import functions as F

exploded = df_carts.select("id", "coupon_codes", F.explode("attributes").alias("key", "v"))

flattened = exploded.select("id", "key", "v", F.explode_outer("coupon_codes").alias("code"))

valid = (
    flattened
    .filter(F.col("code").isNotNull() & (~F.col("code").eqNullSafe(F.lit("BUNDLE"))))
    .select(
        "*",
        F.lower("code").alias("code_lower"),
        F.coalesce("code", F.lit("UNKNOWN")).alias("resolved_code"),
    )
)

valid_carts = (
    valid
    .filter(F.col("code").isNotNull())
    .select(
        "id",
        "v",
        "code",
        "code_lower",
        "resolved_code",
        F.lower("key").alias("key_lower"),
        F.coalesce("key", F.lit("DIRECT")).alias("resolved_key"),
    )
)

result = (
    valid_carts
    .select("id", "v", "resolved_code", "key_lower")
    .orderBy(F.col("id").desc())
)
