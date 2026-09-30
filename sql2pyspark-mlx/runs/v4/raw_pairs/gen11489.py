from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") >= 1)
)
