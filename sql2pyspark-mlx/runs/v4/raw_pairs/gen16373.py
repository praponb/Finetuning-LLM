from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.total_quantity") >= 31)
    .select(F.col("p.id"), F.col("p.price"), F.col("ps.total_quantity"))
    .orderBy(F.col("id").desc())
)
