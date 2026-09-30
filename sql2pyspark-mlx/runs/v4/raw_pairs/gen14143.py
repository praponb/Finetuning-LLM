from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("sqft").between(7897, 9959)).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_semi")
    .select(F.col("i.id"), F.col("i.quantity"), F.col("i.store_id"))
    .orderBy(F.col("store_id").desc())
)
