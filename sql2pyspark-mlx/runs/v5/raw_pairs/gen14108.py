from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.size("coupon_codes") > 2)
    .select(
        "id",
        "coupon_codes",
        "attributes",
        F.transform("items", lambda item: item["product_id"]).alias("product_ids"),
        F.col("ship_to.country").alias("ship_country"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("none")).alias("utm_source_or_default"),
    )
)
