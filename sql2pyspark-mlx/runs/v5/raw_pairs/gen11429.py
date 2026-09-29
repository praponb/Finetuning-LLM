from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w_hist = Window.partitionBy("status")
w3 = Window.partitionBy("order_id").orderBy("amount", "id")
w4 = Window.orderBy(F.col("amount").desc_nulls_first())
w5 = Window.partitionBy("method", "status").orderBy("amount_share", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("method").orderBy("status_sum_amount", "id")
w7 = Window.orderBy(F.col("prev_status_sum_amount").desc(), "id")
w8 = Window.orderBy(F.col("last4_sum_amount_cume_dist").desc_nulls_last())
w9 = Window.orderBy(F.col("prev_status_sum_amount").asc_nulls_first())
w10 = Window.orderBy("last4_avg_amount_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("last4_sum_amount_cume_dist", "id")
w12 = Window.orderBy("last4_avg_amount_cume_dist", "id").rowsBetween(-6, -1)
w13 = Window.orderBy("running_prev_status_sum_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("prev_status_sum_amount", "id")
w15 = Window.orderBy(F.col("last4_sum_amount_cume_dist_bucket").asc_nulls_first())
w16 = Window.orderBy(F.col("last4_sum_amount_cume_dist_cume_dist").desc(), "id")
w17 = Window.orderBy(F.col("last4_sum_amount_cume_dist_bucket").asc_nulls_last())
w_running = Window.orderBy(F.col("prev_status_sum_amount_rank_avg_6_prior").desc(), "id")
w19 = Window.orderBy(F.col("last4_sum_amount_cume_dist").desc())
w20 = Window.orderBy("last4_sum_amount_cume_dist_bucket_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w21 = Window.orderBy(F.col("last4_sum_amount_cume_dist").desc(), "id")

metrics = (
    df_payments
    .filter(F.col("amount") < 920)
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.sum("amount").over(w_hist).alias("status_sum_amount"),
        F.lag("amount", 1).over(w3).alias("prev_amount"),
        F.cume_dist().over(w4).alias("amount_cume_dist"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "prev_amount",
        F.count("amount_cume_dist").over(w5).alias("last4_count_amount_cume_dist"),
        F.avg("amount_cume_dist").over(w5).alias("last4_avg_amount_cume_dist"),
        F.min("amount_cume_dist").over(w5).alias("last4_min_amount_cume_dist"),
        F.sum("amount_cume_dist").over(w5).alias("last4_sum_amount_cume_dist"),
        F.lag("status_sum_amount", 2).over(w6).alias("prev_status_sum_amount"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.ntile(10).over(w7).alias("prev_status_sum_amount_bucket"),
        F.percent_rank().over(w8).alias("last4_sum_amount_cume_dist_percent_rank"),
    )
)

metrics_payments = (
    with_windows
    .select(
        "*",
        F.rank().over(w9).alias("prev_status_sum_amount_rank"),
        F.sum("prev_status_sum_amount").over(w10).alias("running_prev_status_sum_amount"),
        F.ntile(5).over(w11).alias("last4_sum_amount_cume_dist_bucket"),
        F.cume_dist().over(w8).alias("last4_sum_amount_cume_dist_cume_dist"),
    )
)

with_windows_payments = (
    metrics_payments
    .select(
        "id",
        "last4_avg_amount_cume_dist",
        "last4_min_amount_cume_dist",
        "last4_sum_amount_cume_dist",
        "prev_status_sum_amount",
        "prev_status_sum_amount_bucket",
        "prev_status_sum_amount_rank",
        "last4_sum_amount_cume_dist_bucket",
        "last4_sum_amount_cume_dist_cume_dist",
        F.avg("prev_status_sum_amount_rank").over(w12).alias("prev_status_sum_amount_rank_avg_6_prior"),
        F.first("last4_avg_amount_cume_dist", ignorenulls=True).over(w13).alias("first_known_last4_avg_amount_cume_dist"),
        F.row_number().over(w_part).alias("row_seq"),
        F.cume_dist().over(w15).alias("last4_sum_amount_cume_dist_bucket_cume_dist"),
        F.row_number().over(w16).alias("row_seq_2"),
    )
)

filtered = (
    with_windows_payments
    .filter(F.col("last4_avg_amount_cume_dist") > 0.38)
    .withColumn("last4_min_amount_cume_dist_minus_last4_sum_amount_cume_dist", (F.col("last4_min_amount_cume_dist") - F.col("last4_sum_amount_cume_dist")))
    .withColumn("last4_avg_amount_cume_dist_band", F.when(F.col("last4_avg_amount_cume_dist") >= 0.87, "High").when(F.col("last4_avg_amount_cume_dist") >= 0.17, "Medium").otherwise("Low"))
)

with_windows_payments_2 = (
    filtered
    .select(
        "id",
        "last4_min_amount_cume_dist",
        "last4_sum_amount_cume_dist",
        "prev_status_sum_amount",
        "prev_status_sum_amount_bucket",
        "prev_status_sum_amount_rank",
        "last4_sum_amount_cume_dist_bucket",
        "last4_sum_amount_cume_dist_cume_dist",
        "prev_status_sum_amount_rank_avg_6_prior",
        "first_known_last4_avg_amount_cume_dist",
        "row_seq",
        "last4_sum_amount_cume_dist_bucket_cume_dist",
        "last4_min_amount_cume_dist_minus_last4_sum_amount_cume_dist",
        "last4_avg_amount_cume_dist_band",
        F.rank().over(w17).alias("last4_sum_amount_cume_dist_bucket_rank"),
        F.row_number().over(w_running).alias("event_seq"),
        F.percent_rank().over(w19).alias("last4_sum_amount_cume_dist_percent_rank"),
        F.last("prev_status_sum_amount_rank", ignorenulls=True).over(w20).alias("last_known_prev_status_sum_amount_rank"),
        F.row_number().over(w21).alias("row_seq_3"),
    )
)

result = (
    with_windows_payments_2
    .select(
        "id",
        "last4_min_amount_cume_dist",
        "last4_sum_amount_cume_dist",
        "prev_status_sum_amount",
        "prev_status_sum_amount_rank",
        "last4_sum_amount_cume_dist_bucket",
        "last4_sum_amount_cume_dist_cume_dist",
        "prev_status_sum_amount_rank_avg_6_prior",
        "first_known_last4_avg_amount_cume_dist",
    )
    .orderBy(F.col("prev_status_sum_amount_rank").desc_nulls_last())
)
