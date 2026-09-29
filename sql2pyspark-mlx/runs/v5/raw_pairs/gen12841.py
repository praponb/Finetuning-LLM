from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 1)
    .groupBy("store_id", "product_id")
    .agg(F.count("quantity").alias("quantity_count"))
    .filter(F.col("quantity_count") >= 4)
    .orderBy(F.col("store_id").desc())
)
