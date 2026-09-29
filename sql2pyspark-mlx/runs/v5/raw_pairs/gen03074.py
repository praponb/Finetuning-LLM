from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.country"))
    .agg(
        F.count("*").alias("order_item_count"),
        F.countDistinct(F.col("oi.quantity")).alias("unique_products"),
    )
    .orderBy(F.col("order_item_count").desc())
)
