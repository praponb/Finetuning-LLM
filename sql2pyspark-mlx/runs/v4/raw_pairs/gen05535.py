from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3]) & F.col("last_restock").between("2024-05-15", "2025-06-29"))
    .groupBy("product_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
    .orderBy("product_id")
)
