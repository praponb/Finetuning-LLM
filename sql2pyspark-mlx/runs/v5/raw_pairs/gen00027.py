from pyspark.sql import functions as F

result = (
    df_orders
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["city", "region", "status"], ["city", "region"], ["city"], []],
        F.col("p.city").alias("city"),
        F.col("p.region").alias("region"),
        F.col("a.status").alias("status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("a.shipping_fee")).alias("avg_shipping_fee"),
        F.max(F.col("a.amount")).alias("max_amount"),
    )
)
