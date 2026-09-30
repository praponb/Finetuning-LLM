from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("unknown")).alias("utm_source_or_default"),
        F.transform("items", lambda item: item["product_id"]).alias("product_ids"),
        F.col("ship_to.country").alias("ship_country"),
        F.col("ship_to.zip").alias("ship_zip"),
    )
)
