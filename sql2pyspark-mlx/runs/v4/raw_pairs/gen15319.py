from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
        F.coalesce(F.col("attributes").getItem("device"), F.lit("none")).alias("device_or_default"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.col("ship_to.city").alias("ship_city"),
        F.array_contains(F.map_keys("attributes"), "device").alias("has_device"),
    )
)

result = (
    unpacked
    .groupBy("ship_city", F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.countDistinct("ship_city").alias("distinct_ship_cities"),
        F.round(F.sum("max_item_price") / F.nullif(F.count(F.when(F.col("ship_city") == "London", 1)), F.lit(0)), 2).alias("max_item_price_per_london"),
        F.max("created_at").alias("last_seen"),
    )
    .orderBy(F.col("activity_day").desc_nulls_first())
)
