from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(~F.col("s.supplier_name").like("%Imports%"))
    .groupBy(
        F.when(F.col("s.rating") >= 4.3, "high").when(F.col("s.rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count(F.col("oi.quantity")).alias("quantity_count"),
        F.countDistinct(F.col("oi.quantity")).alias("unique_products"),
    )
)
