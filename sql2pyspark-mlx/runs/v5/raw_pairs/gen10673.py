from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter(F.col("store_id") == 1)

store_id_summary = (
    filtered_inventory
    .groupBy("store_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = store_id_summary.orderBy(F.col("min_quantity").desc())
