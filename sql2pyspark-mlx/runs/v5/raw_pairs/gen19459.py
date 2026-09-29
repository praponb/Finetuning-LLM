from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.filter(F.col("supplier_id").isin([7, 8, 12, 36])).alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left_anti")
    .select(
        F.col("inventory.id"),
        F.col("inventory.store_id"),
        F.col("inventory.quantity"),
        F.col("inventory.product_id"),
    )
)
