from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.array_contains("coupon_codes", "VIP").alias("used_vip"),
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.size("items").alias("num_items"),
    )
)
