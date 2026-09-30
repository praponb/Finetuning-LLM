from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("last_restock").between("2023-03-23", "2024-04-01")).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_semi")
    .filter(F.col("s.sqft") <= 7467)
    .select(F.col("s.id"), F.col("s.opened_date"), F.col("s.region"), F.col("s.city"))
)
