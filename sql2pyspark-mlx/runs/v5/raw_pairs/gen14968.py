from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.revenue") >= 3600)
    .groupBy(F.col("ss.store_id"), F.col("ss.product_id"))
    .agg(F.avg(F.col("i.quantity")).alias("avg_quantity"))
    .orderBy(F.col("product_id").desc(), "avg_quantity")
)
