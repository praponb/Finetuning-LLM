from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.store_id").isin([2, 3, 4, 6]))
    .select(
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("ss.store_id").alias("store_sale_store_id"),
        F.col("ss.revenue"),
        F.date_trunc("YEAR", F.col("i.last_restock")).alias("last_restock_period"),
    )
    .orderBy(F.col("store_id").desc(), "last_restock")
)
