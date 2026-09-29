from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3]))
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.avg_quantity") > 42)
    .select(F.col("p.price"), F.col("ps.total_quantity"), F.col("ps.avg_quantity"))
)
