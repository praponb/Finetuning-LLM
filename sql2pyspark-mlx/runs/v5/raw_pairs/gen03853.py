from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        "attributes",
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.col("ship_to.country").alias("ship_country"),
        F.size("attributes").alias("num_attributes"),
    )
)
