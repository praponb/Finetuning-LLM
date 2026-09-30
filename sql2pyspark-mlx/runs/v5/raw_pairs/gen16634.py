from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.region").alias("region"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("src.quantity")).alias("avg_quantity"),
    )
)
