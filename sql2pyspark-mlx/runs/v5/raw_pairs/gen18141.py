from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "customer_id").orderBy(F.col("discount").asc_nulls_last(), "id")
w2 = Window.partitionBy("quarter_start").orderBy(F.col("avg_shipping_fee").desc())

latest = (
    df_orders
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy(
        F.date_trunc("QUARTER", "order_date").alias("quarter_start"),
        F.when(F.col("discount") >= 0.37, "PREMIUM").otherwise("STANDARD").alias("discount_tier"),
    )
    .agg(
        F.avg("shipping_fee").alias("avg_shipping_fee"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .withColumn("avg_shipping_fee_dense_rank", F.dense_rank().over(w2))
)
