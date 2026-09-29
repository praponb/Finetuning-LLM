from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "attributes",
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.col("attributes")["device"].alias("device"),
        F.transform("items", lambda x: x["product_id"]).alias("product_ids"),
    )
)
