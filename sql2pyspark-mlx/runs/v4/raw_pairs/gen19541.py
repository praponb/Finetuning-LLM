from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .filter(F.col("inventory.product_id").isin([5, 6, 7]) & F.col("inventory.product_id").isin([2, 4]))
    .select(
        F.col("inventory.last_restock"),
        F.col("inventory.store_id"),
        F.col("inventory.quantity"),
        F.col("store_sales.units"),
        F.col("store_sales.revenue"),
    )
)
