from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter((F.col("i.last_restock") < "2023-10-01") & F.col("i.store_id").isin([1, 6]))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("ss.product_id"),
        F.col("ss.revenue"),
    )
)
