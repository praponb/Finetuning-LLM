from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .groupBy(F.col("p.category"), F.col("p.brand"))
    .agg(
        F.min(F.col("oi.quantity")).alias("min_quantity"),
        F.max(F.col("oi.quantity")).alias("max_quantity"),
    )
    .orderBy("max_quantity", "category")
)
