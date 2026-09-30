from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.col("attributes").getItem("device").alias("device"),
    )
)

result = (
    parsed
    .withColumn("max_item_price_tier", F.when(F.col("max_item_price") >= 190, "PREMIUM").otherwise("STANDARD"))
    .cube("max_item_price_tier")
    .agg(
        F.percentile_approx("max_item_price", 0.95).alias("max_item_price_p95"),
        F.count("*").alias("row_count"),
        F.percentile_approx("max_item_price", 0.5).alias("max_item_price_p50"),
        F.round(F.avg("max_item_price"), 2).alias("avg_max_item_price"),
        F.percentile_approx("max_item_price", [0.25, 0.5, 0.75]).alias("max_item_price_quartiles"),
        F.grouping("max_item_price_tier").alias("grouping_id"),
    )
    .filter(F.col("max_item_price_p50") > 0)
    .orderBy("max_item_price_tier")
)
