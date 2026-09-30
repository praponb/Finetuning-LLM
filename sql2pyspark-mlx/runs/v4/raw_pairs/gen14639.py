from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.alias("inventory"), F.col("products.id") == F.col("inventory.product_id"), "left")
    .select(
        F.col("products.launch_date"),
        F.col("products.supplier_id"),
        F.col("inventory.last_restock"),
        F.col("inventory.quantity"),
    )
)
