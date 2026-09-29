from pyspark.sql import functions as F

recent_order_items = df_order_items.filter(F.col("product_id").isin([16, 18, 26]))

order_items_stats = (
    recent_order_items
    .groupBy("product_id")
    .agg(
        F.count("*").alias("cnt"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    order_items_stats
    .filter(F.col("cnt") >= 6)
    .select("product_id", "cnt", "quantity_count")
    .orderBy("cnt", F.col("quantity_count").desc())
)
