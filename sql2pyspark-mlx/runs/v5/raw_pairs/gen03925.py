from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["region", "city"], ["region"], []],
        F.col("d.region").alias("region"),
        F.col("d.city").alias("city"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.quantity")).alias("max_quantity"),
        F.grouping_id().alias("level_id"),
    )
)
