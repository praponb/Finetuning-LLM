from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.year("last_restock") >= 2024)
    .groupBy("store_id", "product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .orderBy(F.col("product_id").desc(), "store_id")
    .limit(20)
)
