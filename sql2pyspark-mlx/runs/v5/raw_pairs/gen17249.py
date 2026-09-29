from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "right")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .filter(F.col("inventory.last_restock") <= "2023-04-28")
    .select(
        F.col("inventory.id"),
        F.col("inventory.product_id"),
        F.col("products.cost"),
        F.col("products.launch_date"),
        F.col("suppliers.supplier_name"),
    )
)
