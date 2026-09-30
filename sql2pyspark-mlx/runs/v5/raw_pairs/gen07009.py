from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("status_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "order_id")
w3 = Window.partitionBy("order_status").orderBy(F.col("status_length").desc(), "id")
w4 = Window.partitionBy("method").orderBy("status_length")
w5 = Window.partitionBy("order_id", "order_status").orderBy("shipping_fee", "id")
w6 = Window.partitionBy("method").orderBy("shipping_fee", "id")
w7 = Window.partitionBy("method").orderBy("cumulative_status_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("method").orderBy("cumulative_status_length", "id")
w9 = Window.orderBy("cumulative_status_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-07-01 09:00:00").cast("timestamp")) & F.col("order_id").isin([19, 30, 31, 33]))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.length("status").alias("status_length"),
        F.date_format("paid_at", "MMM yyyy").alias("paid_at_label"),
    )
)

with_dims = (
    filtered
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.method",
        "a.paid_at",
        "a.status_length",
        "a.paid_at_label",
        F.col("o.channel"),
        F.col("o.status").alias("order_status"),
    )
)

metrics = (
    with_dims
    .select(
        "id",
        "order_id",
        "method",
        "status_length",
        "paid_at_label",
        "channel",
        "order_status",
        F.sum("status_length").over(w).alias("cumulative_status_length"),
        F.stddev_samp("status_length").over(w2).alias("channel_stddev_samp_status_length"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

deduped = (
    metrics
    .withColumn("rn", F.rank().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "order_id",
        "method",
        "paid_at_label",
        "order_status",
        "cumulative_status_length",
        "seq_no",
    )
)

joined = (
    deduped
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.shipping_fee"))
)

with_windows = (
    joined
    .filter(F.col("order_status").isin(["COMPLETED", "CANCELLED", "RETURNED"]))
    .select(
        "*",
        F.lag("seq_no", 1).over(w5).alias("prev_seq_no"),
        F.lag("shipping_fee", 2).over(w6).alias("prev_shipping_fee"),
        F.last("shipping_fee", ignorenulls=True).over(w7).alias("last_known_shipping_fee"),
        F.lag("seq_no", 1).over(w8).alias("prev_seq_no_2"),
        F.first("order_status", ignorenulls=True).over(w9).alias("first_known_order_status"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("prev_seq_no_2") >= 9, "HIGH").otherwise("LOW").alias("prev_seq_no_2_tier"),
        "method",
    )
    .agg(
        F.avg(F.when(F.col("method") == "bank_transfer", F.col("seq_no"))).alias("bank_transfer_avg_seq_no"),
        F.round(F.avg("prev_seq_no_2"), 2).alias("avg_prev_seq_no_2"),
        F.percentile_approx("shipping_fee", 0.5).alias("shipping_fee_p50"),
        F.count(F.when(F.col("order_status") == "COMPLETED", 1)).alias("completed_count"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("bank_transfer_avg_seq_no") < 1000000)
)
