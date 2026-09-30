from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("suppliers.rating").between(2.2, 3.4))
    .select(
        F.col("inventory.product_id"),
        F.col("inventory.quantity"),
        F.col("products.product_name"),
        F.col("products.category"),
        F.col("suppliers.supplier_name"),
    )
    .orderBy(F.col("quantity").desc())
)
