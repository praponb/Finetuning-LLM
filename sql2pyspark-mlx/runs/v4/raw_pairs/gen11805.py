from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .groupBy(F.col("ss.store_id"))
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .orderBy(F.col("total_quantity").desc(), F.col("store_id").desc())
    .limit(1)
)
