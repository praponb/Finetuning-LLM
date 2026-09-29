from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter((F.col("ss.revenue") <= 2000) | F.col("i.product_id").isin([1, 3, 6]))
    .select(
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("ss.store_id").alias("store_sale_store_id"),
        F.col("ss.units"),
        F.col("ss.revenue"),
        F.datediff(F.current_date(), F.col("i.last_restock")).alias("days_since_last_restock"),
    )
)
