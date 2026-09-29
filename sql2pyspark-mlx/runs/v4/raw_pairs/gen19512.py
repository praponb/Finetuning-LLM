from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("store_name").isNotNull()).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("i.quantity").between(110, 118))
    .select(F.col("i.id"), F.col("i.store_id"), F.col("i.last_restock"))
    .orderBy(F.col("id").desc(), "store_id")
    .limit(50)
)
