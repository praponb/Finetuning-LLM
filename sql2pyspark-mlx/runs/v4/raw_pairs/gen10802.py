from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .filter((F.col("inventory.last_restock") <= "2024-11-01") & (F.col("inventory.product_id") == 10))
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("products.cost"),
        F.col("products.stock_qty"),
    )
    .orderBy("quantity")
)
