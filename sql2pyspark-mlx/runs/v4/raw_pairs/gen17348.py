from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "channel").orderBy("amount", "id")
w4 = Window.partitionBy("channel", "store_id").orderBy(F.col("discount").desc(), "id")
w5 = Window.partitionBy("status", "store_id")
w6 = Window.partitionBy("store_id").orderBy(F.col("seq_no_per_store").desc())

with_windows = (
    df_orders
    .withColumn("channel_sum_shipping_fee", F.sum("shipping_fee").over(w))
    .withColumn("cumulative_shipping_fee", F.sum("shipping_fee").over(w2))
    .withColumn("prev_amount", F.lag("amount", 2, 0).over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("status_stddev_samp_amount", F.stddev_samp("amount").over(w5))
)

result = (
    with_windows
    .groupBy(
        "store_id",
        F.date_trunc("YEAR", "order_date").alias("year_start"),
        "seq_no",
    )
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("amount_per_web"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
        F.count("*").alias("num_rows"),
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("seq_no_per_store"),
        F.avg(F.when(F.col("channel") == "partner", F.col("cumulative_shipping_fee"))).alias("partner_avg_cumulative_shipping_fee"),
    )
    .withColumn("seq_no_per_store_dense_rank", F.dense_rank().over(w6))
    .orderBy(F.col("seq_no").desc_nulls_last())
)
