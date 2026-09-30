from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("product_id").isin([2, 3, 22, 33]))

order_items_stats = (
    filtered_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.max("unit_price").alias("max_unit_price"))
)

result = order_items_stats.select("quantity_bucket", "max_unit_price")
