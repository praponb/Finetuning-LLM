from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 5]))
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("ProductStats"), F.col("products.id") == F.col("ProductStats.product_id"), "inner")
    .select(
        F.col("products.supplier_id"),
        F.col("products.launch_date"),
        F.col("ProductStats.quantity_count"),
        F.col("ProductStats.unique_products"),
    )
)
