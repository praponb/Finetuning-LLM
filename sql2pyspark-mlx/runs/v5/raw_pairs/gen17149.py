from pyspark.sql import functions as F

result = (
    df_orders
    .alias("s")
    .join(df_stores.alias("dim"), F.col("s.store_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["city", "channel"], ["city"], []],
        F.col("dim.city").alias("city"),
        F.col("s.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("s.amount")), 2).alias("avg_amount"),
        F.min(F.col("s.shipping_fee")).alias("min_shipping_fee"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("cnt").desc_nulls_last())
)
