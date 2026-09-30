from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("inventory.store_id"),
        F.col("store_sales.product_id"),
        F.date_sub(F.col("inventory.last_restock"), 90).alias("last_restock_minus_90d"),
    )
    .orderBy(F.col("product_id").desc())
)
