from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 32)
    .groupBy("channel")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.max("shipping_fee").alias("max_shipping_fee"),
    )
    .orderBy("max_shipping_fee", F.col("channel").desc())
)
