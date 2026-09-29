from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "ship_to",
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.coalesce(F.col("attributes").getItem("device"), F.lit("direct")).alias("device_or_default"),
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
    )
)
