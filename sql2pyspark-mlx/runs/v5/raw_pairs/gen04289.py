from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("inventory.product_id"),
        F.col("products.price"),
        F.col("suppliers.rating"),
        F.round((F.col("inventory.quantity") * 1.15), 2).alias("adjusted_quantity"),
    )
    .orderBy(F.col("price").desc(), "id")
    .limit(10)
)
