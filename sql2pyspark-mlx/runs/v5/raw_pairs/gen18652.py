from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(107, 211) & (F.col("last_restock") <= "2025-08-06"))
    .groupBy("product_id")
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("row_count").desc(), "product_id")
    .limit(50)
)
