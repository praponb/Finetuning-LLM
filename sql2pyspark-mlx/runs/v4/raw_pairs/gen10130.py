from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
        F.array_contains("coupon_codes", "VIP").alias("used_vip"),
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.exists("items", lambda li: li["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    unpacked
    .groupBy("customer_id", F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.percentile_approx("weight_kg", [0.25, 0.5, 0.75]).alias("weight_kg_quartiles"),
        F.percentile_approx("max_item_price", 0.5).alias("max_item_price_p50"),
        F.percentile_approx("max_item_price", 0.9).alias("max_item_price_p90"),
        F.percentile_approx("max_item_price", 0.5).alias("max_item_price_p50_2"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.avg("max_item_price").alias("avg_max_item_price"),
    )
    .orderBy(F.col("customer_id").desc_nulls_last())
)
