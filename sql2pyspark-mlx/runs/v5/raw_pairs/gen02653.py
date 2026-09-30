from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .filter(F.col("product_id").isin([3, 4]))
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.avg_quantity") >= 90)
    .select(F.col("p.launch_date"), F.col("p.stock_qty"), F.col("ps.avg_quantity"))
)
