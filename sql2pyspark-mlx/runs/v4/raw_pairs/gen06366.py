from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("i.last_restock").between("2025-02-06", "2025-08-14") & (F.col("i.last_restock") > "2023-10-02"))
    .select(
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("i.store_id"),
    )
    .orderBy(F.col("store_id").desc())
)
