from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 3)
    .groupBy("store_id")
    .agg(F.min("last_restock").alias("min_last_restock"))
    .orderBy("store_id")
    .limit(50)
)
