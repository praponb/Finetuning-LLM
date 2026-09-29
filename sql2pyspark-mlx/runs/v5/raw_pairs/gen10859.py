from pyspark.sql import functions as F

result = (
    df_orders
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["city", "region"], ["city"], []],
        F.col("d.city").alias("city"),
        F.col("d.region").alias("region"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("e.discount")), 2).alias("avg_discount"),
    )
)
