from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "channel").orderBy("amount", "id")
w2 = Window.partitionBy("channel", "customer_id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee_stddev").desc())

with_windows = (
    df_orders
    .withColumn("prev_discount", F.lag("discount", 1).over(w))
    .withColumn("corr_shipping_fee_amount", F.corr("shipping_fee", "amount").over(w2))
)

result = (
    with_windows
    .groupBy(
        "customer_id",
        F.when(F.col("prev_discount") >= 0.31, "HIGH").otherwise("LOW").alias("prev_discount_tier"),
        "store_id",
    )
    .agg(
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.count_if(F.col("channel") == "store").alias("store_events"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("amount")).otherwise(0)).alias("returned_amount"),
        F.avg("amount").alias("avg_amount"),
        F.max(F.when(F.col("status") == "CANCELLED", F.col("corr_shipping_fee_amount"))).alias("cancelled_max_corr_shipping_fee_amount"),
    )
    .withColumn("shipping_fee_stddev_dense_rank", F.dense_rank().over(w3))
)
