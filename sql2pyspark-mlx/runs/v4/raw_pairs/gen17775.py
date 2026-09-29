from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter((F.col("i.last_restock") >= "2025-07-01") & (F.col("i.last_restock") >= "2023-03-01"))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("ss.store_id").alias("store_sale_store_id"),
        (F.col("ss.revenue") + F.col("ss.units")).alias("revenue_per_unit"),
    )
    .orderBy("store_id")
)
