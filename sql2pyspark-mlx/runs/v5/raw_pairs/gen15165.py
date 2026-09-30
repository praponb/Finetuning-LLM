from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("oi.product_id").isin([4, 23, 29]))
    .groupBy(
        F.when(F.col("s.rating") >= 4.2, "high").when(F.col("s.rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count(F.col("oi.quantity")).alias("quantity_count"))
)
