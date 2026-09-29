from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .groupBy("product_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.avg_quantity") >= 33)
    .select(F.col("p.price"), F.col("p.product_name"), F.col("ps.avg_quantity"))
    .orderBy("product_name")
)
