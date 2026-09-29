from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        F.coalesce(F.col("attributes")["utm_source"], F.lit("none")).alias("utm_source_or_default"),
        F.col("ship_to.country").alias("ship_country"),
        F.col("attributes")["device"].alias("device"),
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
    )
)
