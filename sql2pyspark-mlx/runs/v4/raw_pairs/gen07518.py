from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .select(
        F.col("inventory.last_restock"),
        F.col("inventory.quantity"),
        F.col("inventory.store_id"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.store_id").alias("store_sale_store_id"),
    )
    .orderBy("quantity")
)
