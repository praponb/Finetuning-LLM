from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.quantity") <= 226)
    .groupBy(F.col("ss.product_id"))
    .agg(F.count(F.col("i.quantity")).alias("quantity_count"))
    .orderBy(F.col("product_id").desc())
    .limit(50)
)
