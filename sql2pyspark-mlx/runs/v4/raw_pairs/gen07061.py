from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.revenue") >= 1200)
    .groupBy(F.col("ss.store_id"))
    .agg(
        F.min(F.col("i.last_restock")).alias("min_last_restock"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc(), "store_id")
    .limit(1)
)
