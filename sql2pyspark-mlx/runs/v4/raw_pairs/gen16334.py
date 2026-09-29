from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.store_name"), F.col("d.city"))
)
