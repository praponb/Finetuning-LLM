from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(3, 5) & (F.col("quantity") >= 3) & (F.col("unit_price") <= 270))
    .groupBy("order_id", "product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("order_item_count"),
    )
)
