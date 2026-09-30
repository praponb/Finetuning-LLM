from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.filter(F.col("order_id") == 33).alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_anti")
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("p.cost"), F.col("p.category"))
    .orderBy(F.col("supplier_id").desc())
)
