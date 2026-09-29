from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_refunded", "id")
w2 = Window.orderBy("paid_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.orderBy(F.col("prev_is_refunded").desc_nulls_last())
w5 = Window.orderBy("prev_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("amount", "id")
w7 = Window.orderBy("prev_amount", "id")
w8 = Window.orderBy("cumulative_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("prev_amount_bucket", "id").rowsBetween(-5, Window.currentRow)
w10 = Window.orderBy(F.col("cumulative_amount").desc(), "id")

valid = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-08-03 23:00:00").cast("timestamp")) & F.col("order_id").isin([8, 11, 12, 20]))
    .select(
        "id",
        "amount",
        "paid_at",
        F.when(F.col("status") == "REFUNDED", 1).otherwise(0).alias("is_refunded"),
        F.to_date("paid_at").alias("paid_at_date"),
        F.coalesce("method", F.lit("OTHER")).alias("method_filled"),
    )
)

with_windows = (
    valid
    .filter(F.col("is_refunded").between(46, 52))
    .withColumn("prev_amount", F.lag("amount").over(w))
    .withColumn("prev_is_refunded", F.lag("is_refunded").over(w))
    .withColumn("next_is_refunded", F.lead("is_refunded").over(w))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("running_amount", F.sum("amount").over(w2))
)

metrics = (
    with_windows
    .withColumn("prev_amount_count_30d", F.count("prev_amount").over(w3))
    .withColumn("prev_is_refunded_rank", F.rank().over(w4))
    .withColumn("cumulative_amount", F.sum("amount").over(w5))
)

metrics_payments = (
    metrics
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("prev_amount_bucket", F.ntile(5).over(w7))
    .withColumn("last_known_cumulative_amount", F.last("cumulative_amount", ignorenulls=True).over(w8))
)

metrics_payments_2 = (
    metrics_payments
    .withColumn("running_sum_running_amount", F.sum("running_amount").over(w2))
    .withColumn("running_count_running_amount", F.count("running_amount").over(w2))
    .withColumn("running_avg_running_amount", F.avg("running_amount").over(w2))
    .withColumn("running_min_running_amount", F.min("running_amount").over(w2))
)

enriched = (
    metrics_payments_2
    .select(
        "id",
        "amount",
        "method_filled",
        "prev_amount",
        "prev_is_refunded",
        "next_is_refunded",
        "seq_no",
        "prev_is_refunded_rank",
        "cumulative_amount",
        "event_seq",
        "prev_amount_bucket",
        "last_known_cumulative_amount",
        "running_sum_running_amount",
        "running_count_running_amount",
        "running_avg_running_amount",
        "running_min_running_amount",
        F.avg("running_min_running_amount").over(w9).alias("running_min_running_amount_avg_5"),
        F.ntile(3).over(w10).alias("cumulative_amount_bucket"),
        F.sum("amount").over(w3).alias("amount_sum_30d"),
    )
)

result = (
    enriched
    .groupBy("seq_no")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("cumulative_amount", 0.5).alias("cumulative_amount_p50"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.percentile_approx("prev_is_refunded", 0.95).alias("prev_is_refunded_p95"),
        F.count("*").alias("cnt"),
        F.percentile_approx("cumulative_amount", [0.25, 0.5, 0.75]).alias("cumulative_amount_quartiles"),
    )
    .orderBy("distinct_ids")
)
