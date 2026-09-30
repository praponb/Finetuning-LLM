from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.product_id").isin([1, 2, 3, 7]))
    .groupBy(F.col("ss.store_id"), F.col("ss.product_id"))
    .agg(
        F.min(F.col("i.last_restock")).alias("first_last_restock"),
        F.min(F.col("i.last_restock")).alias("min_last_restock"),
    )
)
