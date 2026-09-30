from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(F.count("*").alias("total_count"))
    .orderBy("store_id")
)
