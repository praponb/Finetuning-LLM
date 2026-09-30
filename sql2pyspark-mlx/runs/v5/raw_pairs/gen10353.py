from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_semi")
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("p.category"))
    .orderBy("supplier_id", "id")
    .limit(5)
)
