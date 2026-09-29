from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("order_id").isin([17, 32, 39]))
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(F.col("p.id"), F.col("p.cost"), F.col("ps.total_quantity"))
    .orderBy(F.col("total_quantity").desc())
)
