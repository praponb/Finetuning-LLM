from pyspark.sql import functions as F

result = (
    df_orders
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["region", "status"], ["region"], []],
        F.col("p.region").alias("region"),
        F.col("src.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("src.discount")).alias("total_discount"),
    )
)
