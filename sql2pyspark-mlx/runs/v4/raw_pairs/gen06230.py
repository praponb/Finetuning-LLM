from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        "attributes",
        F.array_contains("coupon_codes", "WELCOME5").alias("used_welcome5"),
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.col("attributes")["device"].alias("device"),
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
    )
)
