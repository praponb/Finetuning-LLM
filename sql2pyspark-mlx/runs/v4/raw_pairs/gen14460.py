from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        "ship_to",
        "attributes",
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    unpacked
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.round(F.avg("max_item_price"), 2).alias("avg_max_item_price"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("max_item_price", 0.95).alias("max_item_price_p95"),
        F.round(F.avg("max_item_price"), 2).alias("avg_max_item_price_2"),
        F.round(F.avg("max_item_price"), 2).alias("avg_max_item_price_3"),
    )
)
