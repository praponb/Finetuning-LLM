from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.alias("inventory"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .select(
        F.col("products.price"),
        F.col("inventory.product_id"),
        F.col("inventory.quantity"),
        F.col("inventory.last_restock"),
    )
)
