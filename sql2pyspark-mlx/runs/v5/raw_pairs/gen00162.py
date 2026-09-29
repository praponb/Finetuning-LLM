from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("products.brand").isNotNull())
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("products.brand"),
        F.col("products.category"),
        F.col("suppliers.rating"),
        F.col("suppliers.supplier_name"),
    )
)
