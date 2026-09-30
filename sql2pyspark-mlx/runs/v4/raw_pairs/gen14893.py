from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("order_date", "id")
w_part = Window.partitionBy("status").orderBy("shipping_fee", "id")
w4 = Window.partitionBy("customer_id")
w_part_2 = Window.orderBy("amount", "id")
w6 = Window.partitionBy("channel").orderBy("corr_shipping_fee_discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("channel", "status")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w9 = Window.partitionBy("store_id").orderBy("row_seq", "id")
w10 = Window.orderBy(F.col("channel_avg_amount").desc(), "id")
w_hist = Window.partitionBy("payment_status")
w12 = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("channel").orderBy("shipping_fee", "id").rowsBetween(-6, Window.currentRow)

scored = (
    df_orders
    .withColumn("cumulative_shipping_fee", F.sum("shipping_fee").over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("prev_discount", F.lag("discount", 2, 0).over(w_part))
    .withColumn("corr_shipping_fee_discount", F.corr("shipping_fee", "discount").over(w4))
    .withColumn("prev_amount", F.lag("amount", 1).over(w_part_2))
)

scored_orders = (
    scored
    .select(
        "*",
        F.sum("row_seq").over(w6).alias("running_row_seq"),
        F.avg("amount").over(w7).alias("channel_avg_amount"),
    )
)

with_latest = (
    scored_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("payment_status"))
)

scored_orders_2 = (
    with_latest
    .withColumn("next_amount", F.lead("amount", 2).over(w9))
    .withColumn("channel_avg_amount_bucket", F.ntile(3).over(w10))
)

metrics = (
    scored_orders_2
    .select(
        "*",
        F.stddev_samp("corr_shipping_fee_discount").over(w_hist).alias("payment_status_stddev_samp_corr_shipping_fee_discount"),
        F.sum("prev_discount").over(w12).alias("cumulative_prev_discount"),
        F.avg("corr_shipping_fee_discount").over(w13).alias("corr_shipping_fee_discount_avg_6"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.product_id").isin([1, 5, 33])), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id") == 36), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("row_seq") >= 9, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_rows"),
        F.count(F.when(F.col("payment_status") == "REFUNDED", 1)).alias("refunded_rows"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
)
