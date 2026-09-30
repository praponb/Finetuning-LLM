from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.store_id").isin([2, 4, 5, 6]))
    .groupBy(F.col("ss.store_id"), F.col("ss.product_id"))
    .agg(
        F.max(F.col("i.quantity")).alias("max_quantity"),
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
    .orderBy("quantity_range", F.col("store_id").desc())
)
