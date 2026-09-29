from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") > "2025-04-30") | (F.col("store_id") == 6))
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(F.sum("quantity").alias("total_quantity"))
    .orderBy("total_quantity")
)
