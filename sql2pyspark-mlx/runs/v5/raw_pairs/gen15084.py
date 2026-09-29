from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

base = (
    exploded
    .filter(F.array_contains("coupon_codes", "WELCOME5"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "k",
        F.array_join("coupon_codes", ", ").alias("coupon_codes_text"),
    )
)

cleaned = (
    base
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "coupon_codes",
        "k",
        "coupon_codes_text",
        F.length("k").alias("k_length"),
        F.size("coupon_codes").alias("coupon_codes_count"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_filled"),
    )
)

base_carts = (
    cleaned
    .filter(F.array_contains("coupon_codes", "VIP"))
    .select(
        "id",
        "k",
        "coupon_codes_text",
        "k_length",
        "k_filled",
        F.sort_array("coupon_codes").alias("sorted_coupon_codes"),
        F.coalesce("k", F.lit("OTHER")).alias("resolved_k"),
    )
)

valid = (
    base_carts
    .filter(F.col("k_length") >= 55)
    .select(
        "id",
        "k",
        "coupon_codes_text",
        "k_length",
        "k_filled",
        "resolved_k",
        F.sort_array("sorted_coupon_codes").alias("sorted_sorted_coupon_codes"),
        F.coalesce("k", F.lit("NONE")).alias("k_filled_2"),
    )
)

filtered = (
    valid
    .filter(F.col("k_length") >= 60)
    .withColumn("is_gift", F.when(F.col("k") == "gift", 1).otherwise(0))
)

result = (
    filtered
    .select("id", "k", "coupon_codes_text", "k_length")
    .orderBy("k")
)
