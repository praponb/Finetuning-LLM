from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("channel").isNotNull() & (F.col("store_id") == 22))
    .withColumn("discount_int", F.col("discount").cast("int"))
)

result = (
    valid
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.city").alias("city"), F.col("e.channel").alias("channel"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.discount")).alias("max_discount"),
        F.round(F.avg(F.col("e.shipping_fee")), 2).alias("avg_shipping_fee"),
    )
)
