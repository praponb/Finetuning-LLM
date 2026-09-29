from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "left")
    .filter(F.col("suppliers.rating") < 3.2)
    .select(
        F.col("inventory.id"),
        F.col("inventory.store_id"),
        F.col("inventory.quantity"),
        F.col("products.category"),
        F.col("suppliers.rating"),
    )
)
