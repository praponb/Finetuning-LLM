from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.alias("inventory"), F.col("stores.id") == F.col("inventory.store_id"), "left_anti")
    .select(F.col("stores.id"), F.col("stores.city"))
    .orderBy("city", F.col("id").desc())
    .limit(5)
)
