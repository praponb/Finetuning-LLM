from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.array_contains("coupon_codes", "WELCOME5"))
    .select(
        "id",
        "coupon_codes",
        "attributes",
        F.col("attributes")["device"].alias("device"),
        F.col("attributes")["utm_source"].alias("utm_source"),
    )
)
