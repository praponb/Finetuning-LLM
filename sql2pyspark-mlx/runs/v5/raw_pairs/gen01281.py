from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .filter(F.col("store_id").isin([3, 6]))
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.total_quantity") > 1270)
    .select(F.col("p.id"), F.col("p.brand"), F.col("ps.total_quantity"))
)
