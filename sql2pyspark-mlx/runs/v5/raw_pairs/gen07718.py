from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "attributes",
        F.col("ship_to.country").alias("ship_country"),
        F.coalesce(F.col("attributes")["device"], F.lit("none")).alias("device_or_default"),
    )
)
