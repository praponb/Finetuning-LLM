from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.revenue") >= 69)
    .select(F.col("p.id"), F.col("p.category"), F.col("ps.revenue"))
    .orderBy("id")
)
