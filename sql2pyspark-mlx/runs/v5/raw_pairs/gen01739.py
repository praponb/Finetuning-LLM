from pyspark.sql import functions as F

base = (
    df_order_items
    .filter((F.col("product_id") == 2) & F.col("order_id").isin([11, 16, 21, 31]))
)

order_items_stats = (
    base
    .groupBy(
        F.when(F.col("unit_price") >= 360, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.count("*").alias("order_item_count"))
)

result = order_items_stats.orderBy(F.col("order_item_count").desc())
