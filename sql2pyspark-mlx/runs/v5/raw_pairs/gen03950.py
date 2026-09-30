from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

filtered = (
    lines
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & (F.col("promo").eqNullSafe(F.lit("WELCOME5"))))
    .withColumn("coupon_codes_count", F.size("coupon_codes"))
)

cleaned = (
    filtered
    .filter((F.col("created_at") > F.lit("2025-03-01 16:00:00").cast("timestamp")) & (~F.col("promo").eqNullSafe(F.lit("VIP"))))
    .withColumn("coupon_codes_count_2", F.size("coupon_codes"))
    .withColumn("resolved_promo", F.coalesce("promo", F.lit("UNKNOWN")))
)

valid = (
    cleaned
    .filter(~F.col("promo").eqNullSafe(F.lit("WELCOME5")))
    .withColumn("qty_minus_coupon_codes_count_2", (F.col("qty") - F.col("coupon_codes_count_2")))
)

result = (
    valid
    .select(
        "created_at",
        "price",
        "coupon_codes_count",
        "coupon_codes_count_2",
        "qty_minus_coupon_codes_count_2",
    )
)
