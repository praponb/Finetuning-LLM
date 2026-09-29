from pyspark.sql import functions as F

not_flagged = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.product_id") == 19), "left_anti")
)

result = (
    not_flagged
    .withColumn("discount_tier", F.when(F.col("discount") >= 0.32, "HIGH").otherwise("LOW"))
    .withColumn("shipping_fee_tier", F.when(F.col("shipping_fee") >= 44, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["discount_tier", "shipping_fee_tier"], ["discount_tier"], []],
        "discount_tier",
        "shipping_fee_tier",
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
    )
    .filter(F.col("store_count") > 1)
)
