from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
        F.forall("items", lambda x: x["qty"] <= 5).alias("all_small_qty"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.col("ship_to.zip").alias("ship_zip"),
    )
)

result = (
    unpacked
    .groupBy(F.date_trunc("hour", "created_at").alias("hour_window"))
    .agg(
        F.avg("max_item_price").alias("avg_max_item_price"),
        F.avg("max_item_price").alias("avg_max_item_price_2"),
        F.min("created_at").alias("started_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.min("created_at").alias("first_created_at"),
    )
    .orderBy(F.col("avg_max_item_price_2").desc_nulls_first())
)
