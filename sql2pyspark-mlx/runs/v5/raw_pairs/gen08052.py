from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left_semi")
    .filter(F.col("s.region").isin(["South", "East"]))
    .select(F.col("s.id"), F.col("s.region"), F.col("s.store_name"))
    .orderBy(F.col("id").desc())
)
