from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.sale_date") >= "2025-09-17")
    .groupBy(F.col("ss.store_id"))
    .agg(F.count("*").alias("inventory_count"))
    .orderBy("inventory_count")
)
