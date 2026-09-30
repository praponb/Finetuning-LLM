from pyspark.sql import functions as F

exploded = df_carts.select("id", "coupon_codes", F.explode("attributes").alias("key", "v"))

base = (
    exploded
    .filter(F.col("key").isNotNull())
    .withColumn("clean_key", F.trim("key"))
)

base_carts = (
    base
    .filter(F.array_contains("coupon_codes", "VIP"))
    .select(
        "*",
        F.array_contains("coupon_codes", "WELCOME5").alias("is_welcome5"),
        F.coalesce("key", F.lit("OTHER")).alias("resolved_key"),
    )
)

base_carts_2 = (
    base_carts
    .filter(F.array_contains("coupon_codes", "WELCOME5") & F.col("key").isNotNull())
    .withColumn("key_filled", F.coalesce("key", F.lit("N/A")))
)

base_carts_3 = (
    base_carts_2
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "id",
        "coupon_codes",
        "key",
        "v",
        "clean_key",
        "is_welcome5",
        "resolved_key",
        "key_filled",
        F.array_contains("coupon_codes", "VIP").alias("is_vip"),
        F.trim("key").alias("clean_key_2"),
    )
)

result = (
    base_carts_3
    .select(
        "id",
        "coupon_codes",
        "key",
        "v",
        "clean_key",
        "is_welcome5",
        "resolved_key",
        "clean_key_2",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
