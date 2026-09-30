from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "customer_id").orderBy(F.col("order_date").desc(), "id")

deduped = (
    df_orders
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("shipping_fee") >= 34, "LARGE").otherwise("SMALL").alias("shipping_fee_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("shipping_fee"))).alias("returned_total_shipping_fee"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_rows"),
    )
    .filter(F.col("cancelled_count") >= 1)
    .orderBy("returned_total_shipping_fee")
)
