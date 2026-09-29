from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("opened_date").between("2023-08-25", "2024-03-01")).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_semi")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
    )
    .orderBy(F.col("store_id").desc())
)
