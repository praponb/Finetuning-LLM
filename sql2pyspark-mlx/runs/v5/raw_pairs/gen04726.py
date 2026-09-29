from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status").orderBy("amount", "id")
w2 = Window.partitionBy("method").orderBy("amount")
w3 = Window.partitionBy("order_id").orderBy("prev_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("amount_dense_rank", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("prev_amount", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status")
w_recent = Window.partitionBy("status", "method")
w_recent_2 = Window.orderBy("first_known_amount", "id")
w11 = Window.partitionBy("status").orderBy("amount_dense_rank_avg_5", "id")

metrics = (
    df_payments
    .filter(F.col("paid_at") < "2023-06-27")
    .withColumn("prev_amount", F.lag("amount").over(w_ordered))
    .withColumn("next_amount", F.lead("amount").over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w_ordered))
    .withColumn("amount_dense_rank", F.dense_rank().over(w2))
)

enriched = (
    metrics
    .select(
        "*",
        F.first("method", ignorenulls=True).over(w3).alias("first_known_method"),
        F.avg("prev_amount").over(w4).alias("prev_amount_avg_2"),
        F.avg("amount_dense_rank").over(w5).alias("amount_dense_rank_avg_5"),
        F.first("prev_amount", ignorenulls=True).over(w6).alias("first_known_prev_amount"),
        F.first("amount", ignorenulls=True).over(w7).alias("first_known_amount"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        "prev_amount",
        "next_amount",
        "amount_dense_rank",
        "first_known_method",
        "amount_dense_rank_avg_5",
        "first_known_amount",
        F.avg("first_known_prev_amount").over(w8).alias("status_avg_first_known_prev_amount"),
        F.min("first_known_prev_amount").over(w_recent).alias("status_min_first_known_prev_amount"),
        F.row_number().over(w_recent_2).alias("seq_no"),
    )
)

valid = (
    scored
    .filter(F.col("paid_at") > F.lit("2023-10-06").cast("date"))
    .select(
        "id",
        "paid_at",
        "status",
        "prev_amount",
        "next_amount",
        "amount_dense_rank",
        "first_known_method",
        "amount_dense_rank_avg_5",
        "status_min_first_known_prev_amount",
        "seq_no",
        F.lower("status").alias("status_lower"),
        (F.col("seq_no") - F.col("amount_dense_rank")).alias("seq_no_minus_amount_dense_rank"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "status",
        "amount_dense_rank",
        "status_min_first_known_prev_amount",
        "seq_no",
        "seq_no_minus_amount_dense_rank",
        F.lead("amount_dense_rank").over(w11).alias("next_amount_dense_rank"),
        F.lag("amount_dense_rank").over(w11).alias("prev_amount_dense_rank"),
        F.lag("status_min_first_known_prev_amount").over(w11).alias("prev_status_min_first_known_prev_amount"),
        F.row_number().over(w11).alias("row_seq"),
        F.stddev_samp("amount_dense_rank_avg_5").over(w8).alias("status_stddev_samp_amount_dense_rank_avg_5"),
    )
)

result = (
    with_windows
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("prev_amount_dense_rank"))).alias("success_total_prev_amount_dense_rank"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("status") == "REFUNDED").alias("refunded_events"),
        F.avg(F.when(F.col("status") == "REFUNDED", F.col("amount_dense_rank"))).alias("refunded_avg_amount_dense_rank"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("refunded_events").asc_nulls_last())
)
