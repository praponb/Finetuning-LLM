from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("last_restock").between("2023-07-01", "2025-04-01")).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_semi")
    .filter(F.col("s.region").isin(["East", "West", "Central"]))
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("s.store_name"),
    )
)
