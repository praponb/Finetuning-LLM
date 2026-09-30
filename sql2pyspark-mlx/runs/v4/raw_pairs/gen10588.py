from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.last_restock") > "2024-12-01")
    .groupBy(F.col("ss.store_id"))
    .agg(F.count("*").alias("num_inventory"))
    .filter(F.col("num_inventory") > 2)
    .orderBy("num_inventory")
)
