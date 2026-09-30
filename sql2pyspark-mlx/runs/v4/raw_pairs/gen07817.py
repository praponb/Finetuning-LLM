from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .groupBy(F.col("stores.city"))
    .agg(
        F.max(F.col("inventory.quantity")).alias("max_quantity"),
        F.avg(F.col("inventory.quantity")).alias("avg_quantity"),
    )
    .orderBy("max_quantity", F.col("avg_quantity").desc())
)
