from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.id"), F.col("p.category"))
    .agg(F.count(F.col("oi.id")).alias("order_item_count"))
    .orderBy(F.col("id").desc(), "category")
    .limit(10)
)
