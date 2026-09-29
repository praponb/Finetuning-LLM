from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.store_id").isin([1, 2, 4, 6]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("ss.product_id").alias("store_sale_product_id"),
        F.col("ss.sale_date"),
        F.col("ss.units"),
        F.datediff(F.current_date(), F.col("i.last_restock")).alias("days_since_last_restock"),
    )
)
