from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.product_name"),
        F.col("p.cost"),
        F.col("ps.quantity_count"),
        F.col("ps.cnt"),
    )
)
