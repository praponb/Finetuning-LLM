from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.last_restock").between("2023-04-01", "2023-11-26"))
    .groupBy(F.col("ss.store_id"))
    .agg(F.max(F.col("i.last_restock")).alias("latest_last_restock"))
    .orderBy("store_id")
    .limit(50)
)
