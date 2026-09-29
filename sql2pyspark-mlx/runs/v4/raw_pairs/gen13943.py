from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .filter((F.col("inventory.product_id") == 9) | (F.col("store_sales.sale_date") > "2023-06-01"))
    .select(
        F.col("inventory.quantity"),
        F.col("inventory.last_restock"),
        F.col("inventory.store_id"),
        F.col("store_sales.product_id"),
    )
)
