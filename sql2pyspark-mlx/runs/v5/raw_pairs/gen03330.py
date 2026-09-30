from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.category"), F.col("p.brand"))
    .agg(
        F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"),
        F.countDistinct(F.col("oi.unit_price")).alias("unique_orders"),
    )
    .orderBy(F.col("category").desc())
)
