from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter((F.col("ss.revenue") < 3800) & F.col("ss.store_id").isin([2, 3, 4]))
    .select(
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("ss.sale_date"),
        F.col("ss.store_id").alias("store_sale_store_id"),
    )
)
