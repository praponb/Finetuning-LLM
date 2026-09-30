from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .groupBy(
        F.date_format(F.col("store_sales.sale_date"), "yyyy-MM").alias("sale_month"),
    )
    .agg(
        F.round(F.avg(F.col("inventory.quantity")), 2).alias("avg_quantity"),
        F.min(F.col("inventory.last_restock")).alias("min_last_restock"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
