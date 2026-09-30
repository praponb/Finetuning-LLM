from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .select(F.col("oi.order_id"), F.col("p.category"))
    .orderBy(F.col("category").desc())
)
