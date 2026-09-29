from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["region", "city"], ["region"]],
        F.col("p.region").alias("region"),
        F.col("p.city").alias("city"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("s.quantity")).alias("total_quantity"),
    )
    .orderBy(F.col("rows_total").desc())
)
