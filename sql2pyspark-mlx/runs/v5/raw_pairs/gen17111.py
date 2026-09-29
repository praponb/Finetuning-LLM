from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

base = (
    flattened
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "key",
        "v",
        F.trim("key").alias("clean_key"),
        F.coalesce("key", F.lit("OTHER")).alias("key_clean"),
    )
)

base_carts = (
    base
    .filter(F.hour("created_at").between(11, 17))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "v",
        "key_clean",
        F.array_join("coupon_codes", ", ").alias("coupon_codes_text"),
    )
)

filtered = (
    base_carts
    .filter(F.array_contains("coupon_codes", "VIP"))
    .select(
        "*",
        F.array_contains("coupon_codes", "WELCOME5").alias("is_welcome5"),
        F.col("created_at").cast("date").alias("created_at_day"),
    )
)

result = (
    filtered
    .select(
        "created_at",
        "coupon_codes",
        "key_clean",
        "coupon_codes_text",
        "created_at_day",
    )
)
