from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.filter(F.col("opened_date").between("2023-06-01", "2024-11-15")).alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left_semi")
    .select(
        F.col("inventory.id"),
        F.col("inventory.product_id"),
        F.col("inventory.quantity"),
    )
)
