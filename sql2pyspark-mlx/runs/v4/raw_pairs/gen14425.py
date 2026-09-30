from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("p.supplier_id").isNull() & F.col("p.supplier_id").isNotNull())
    .select(F.col("oi.product_id"), F.col("p.supplier_id"), F.col("p.category"))
)
