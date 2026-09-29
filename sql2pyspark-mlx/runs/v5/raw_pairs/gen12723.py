from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") < "2024-08-01") & (F.col("product_id") == 5))
    .groupBy("store_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.count("*").alias("inventory_count"),
    )
    .orderBy(F.col("inventory_count").desc())
)
