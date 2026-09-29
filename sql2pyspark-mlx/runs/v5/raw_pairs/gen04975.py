from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .cube(F.col("d.region").alias("region"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("s.quantity")).alias("max_quantity"),
    )
)
