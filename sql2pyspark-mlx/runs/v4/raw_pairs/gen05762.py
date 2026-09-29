from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.total_quantity") >= 390)
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.price"),
        F.col("ps.total_quantity"),
        F.col("ps.unique_products"),
    )
)
