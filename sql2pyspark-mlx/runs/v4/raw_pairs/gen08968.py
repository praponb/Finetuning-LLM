from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(F.count("quantity").alias("quantity_count"))
    .orderBy("store_id")
    .limit(20)
)
