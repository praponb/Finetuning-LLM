from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") <= "2025-10-01") & (F.col("store_id") == 4))
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.sum("quantity").alias("total_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("total_quantity") > 1000)
    .orderBy("quantity_range")
)
