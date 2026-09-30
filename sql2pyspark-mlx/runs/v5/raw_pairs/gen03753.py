from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .filter(F.col("inventory.product_id").isin([5, 6, 9, 10]) & F.array_contains(F.col("products.tags"), "premium"))
    .select(
        F.col("inventory.id"),
        F.col("inventory.product_id"),
        F.col("inventory.last_restock"),
        F.col("products.brand"),
        F.col("products.category"),
        F.col("products.product_name"),
    )
)
