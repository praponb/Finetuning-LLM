from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.month(F.col("i.last_restock")).isin([3, 6, 12]))
    .groupBy(F.col("ss.store_id"), F.col("ss.product_id"))
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
    )
    .filter(F.col("max_quantity") >= 100)
)
