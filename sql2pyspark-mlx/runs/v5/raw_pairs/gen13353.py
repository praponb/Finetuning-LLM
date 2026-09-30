from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.alias("order_items"), F.col("products.id") == F.col("order_items.product_id"), "inner")
    .filter(F.col("order_items.product_id") == 1)
    .groupBy(F.col("products.id"), F.col("products.category"))
    .agg(
        F.count(F.col("order_items.id")).alias("order_item_count"),
        F.avg(F.col("order_items.unit_price")).alias("avg_unit_price"),
        F.min(F.col("order_items.quantity")).alias("min_quantity"),
    )
)
