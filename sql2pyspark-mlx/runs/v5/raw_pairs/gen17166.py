from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.min("last_restock").alias("min_last_restock"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("P"), F.col("p.id") == F.col("P.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("P.unique_products"),
        F.col("P.min_last_restock"),
    )
    .orderBy("id")
)
