from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .filter(F.col("product_id") == 8)
    .groupBy("product_id")
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.revenue_range") > 38)
    .select(F.col("p.cost"), F.col("p.category"), F.col("ps.revenue_range"))
    .orderBy(F.col("revenue_range").desc())
)
