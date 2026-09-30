from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.coalesce(F.col("attributes")["device"], F.lit("unknown")).alias("device_or_default"),
        F.col("attributes")["device"].alias("device"),
        F.array_contains("coupon_codes", "VIP").alias("used_vip"),
        F.col("attributes").getItem("gift").alias("gift"),
    )
)
