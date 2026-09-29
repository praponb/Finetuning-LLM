from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        F.col("attributes")["device"].alias("device"),
        F.col("ship_to.country").alias("ship_country"),
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
    )
)
