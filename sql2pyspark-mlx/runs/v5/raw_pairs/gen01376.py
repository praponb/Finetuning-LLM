from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.size("coupon_codes") > 1)
    .select(
        "id",
        "coupon_codes",
        "items",
        "ship_to",
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.coalesce(F.col("attributes")["device"], F.lit("none")).alias("device_or_default"),
        F.exists("items", lambda li: li["price"] > 100).alias("has_premium_item"),
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
        F.col("attributes")["utm_source"].alias("utm_source"),
    )
)
