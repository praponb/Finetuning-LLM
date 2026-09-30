from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("inventory.quantity").between(184, 231))
    .groupBy(F.col("products.category"))
    .agg(
        F.max(F.col("inventory.quantity")).alias("max_quantity"),
        F.count(F.col("inventory.quantity")).alias("quantity_count"),
    )
    .orderBy("category")
)
