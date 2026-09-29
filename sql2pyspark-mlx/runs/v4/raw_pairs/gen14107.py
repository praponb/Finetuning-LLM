from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.filter(F.col("last_restock").between("2023-10-01", "2024-09-01")).alias("inventory"), F.col("stores.id") == F.col("inventory.store_id"), "left_semi")
    .select(
        F.col("stores.id"),
        F.col("stores.opened_date"),
        F.col("stores.store_name"),
        F.col("stores.sqft"),
    )
    .orderBy(F.col("id").desc())
    .limit(1)
)
