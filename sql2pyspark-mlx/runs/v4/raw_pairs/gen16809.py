from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 3, 9, 10]) | (F.col("last_restock") < "2025-10-01"))
    .groupBy("store_id")
    .agg(
        F.count("*").alias("total_count"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") >= 5)
    .orderBy(F.col("total_count").desc())
)
