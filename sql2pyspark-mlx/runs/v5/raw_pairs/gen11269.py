from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .filter(F.col("store_id").isin([2, 5]))
    .groupBy("product_id")
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("t"), F.col("products.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.total_quantity") > 550)
    .select(
        F.col("products.category"),
        F.col("products.product_name"),
        F.col("t.min_last_restock"),
        F.col("t.total_quantity"),
    )
)
