from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .groupBy(
        F.date_format(F.col("store_sales.sale_date"), "yyyy-MM").alias("sale_month"),
        F.col("store_sales.store_id"),
    )
    .agg(
        F.sum(F.col("inventory.quantity")).alias("total_quantity"),
        (F.max(F.col("inventory.quantity")) - F.min(F.col("inventory.quantity"))).alias("quantity_range"),
    )
    .orderBy(F.col("total_quantity").desc())
)
