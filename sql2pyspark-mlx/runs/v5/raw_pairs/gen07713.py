from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.size("attributes").alias("num_attributes"),
        F.array_contains("coupon_codes", "BUNDLE").alias("used_bundle"),
    )
)
