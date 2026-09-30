from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
    )
)

result = (
    parsed
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("max_item_price"), 2).alias("avg_max_item_price"),
        F.min("max_item_price").alias("min_max_item_price"),
    )
    .filter(F.col("avg_max_item_price") > 0)
)
