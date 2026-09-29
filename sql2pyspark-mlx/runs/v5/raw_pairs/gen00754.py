from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        F.aggregate("items", F.lit(0.0), lambda acc, li: acc + li["qty"] * li["price"], lambda acc: F.round(acc, 2)).alias("basket_amount"),
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.exists("items", lambda li: li["price"] > 100).alias("has_premium_item"),
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
    )
)
