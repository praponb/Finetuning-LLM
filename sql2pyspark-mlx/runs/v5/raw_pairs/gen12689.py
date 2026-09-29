from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(((F.col("last_restock") < "2024-08-01") & F.col("store_id").isin([2, 3, 4, 5])) | (F.col("store_id") == 3))
    .groupBy("store_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .orderBy("quantity_count")
)
