from pyspark.sql import functions as F

rollup_totals = (
    df_inventory
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["region", "city"], ["region"], []],
        F.col("d.region").alias("region"),
        F.col("d.city").alias("city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.quantity")).alias("max_quantity"),
    )
)

result = (
    rollup_totals
    .select("region", "num_rows", "max_quantity")
    .orderBy(F.col("region").desc_nulls_last())
)
