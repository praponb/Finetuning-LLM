from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .filter(F.col("suppliers.country") == "DE")
    .groupBy(F.col("products.brand"))
    .agg(F.min(F.col("inventory.last_restock")).alias("first_last_restock"))
)
