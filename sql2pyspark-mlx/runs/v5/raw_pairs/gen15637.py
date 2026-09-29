from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("p.brand"), F.col("p.category"))
    .agg(F.count(F.col("oi.quantity")).alias("quantity_count"))
    .orderBy("category", F.col("brand").desc())
)
