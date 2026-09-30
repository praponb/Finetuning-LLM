from pyspark.sql import functions as F

valid_order_items = df_order_items.filter(F.col("quantity") <= 7)

order_items_stats = (
    valid_order_items
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 380, "high").when(F.col("unit_price") >= 130, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.count("*").alias("row_count"),
    )
)

result = (
    order_items_stats
    .filter(F.col("row_count") > 5)
    .select("order_id", "unit_price_bucket", "total_count", "row_count")
)
