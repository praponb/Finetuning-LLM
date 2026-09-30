from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy(F.col("paid_at").desc(), "id")
w4 = Window.partitionBy("status").orderBy("burst_id", "id")
w5 = Window.partitionBy("status").orderBy("paid_at", "id")
w6 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("new_group_flag").desc_nulls_first())
w8 = Window.orderBy("is_new_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("prev_paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("status").orderBy(F.col("is_new_session").desc(), "id")
w11 = Window.partitionBy("status").orderBy(F.col("burst_id").desc(), "id")
w12 = Window.partitionBy("status")
w13 = Window.orderBy("event_seq_2", "id")
w14 = Window.partitionBy("status").orderBy("session_no")
w15 = Window.orderBy("burst_id", "id")
w16 = Window.orderBy("cumulative_new_group_flag", "id").rowsBetween(-3, -1)

with_prev = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

deduped = (
    islands
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "paid_at", "status", "new_group_flag", "burst_id")
)

latest = (
    deduped
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

prev_events = latest.withColumn("prev_paid_at", F.lag("paid_at").over(w5))

flagged = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands_payments = flagged.withColumn("session_no", F.sum("is_new_session").over(w6))

metrics = (
    islands_payments
    .select(
        "id",
        "paid_at",
        "status",
        "new_group_flag",
        "burst_id",
        "is_new_session",
        "session_no",
        F.percent_rank().over(w7).alias("new_group_flag_percent_rank"),
        F.last("new_group_flag", ignorenulls=True).over(w8).alias("last_known_new_group_flag"),
        F.sum("session_no").over(w9).alias("cumulative_session_no"),
        F.sum("new_group_flag").over(w6).alias("cumulative_new_group_flag"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "status",
        "burst_id",
        "session_no",
        "last_known_new_group_flag",
        "cumulative_session_no",
        "cumulative_new_group_flag",
        F.row_number().over(w10).alias("event_seq"),
        F.row_number().over(w11).alias("event_seq_2"),
    )
)

metrics_payments = (
    with_windows
    .select(
        "*",
        F.sum("session_no").over(w12).alias("status_sum_session_no"),
        F.lead("cumulative_new_group_flag", 1).over(w13).alias("next_cumulative_new_group_flag"),
        F.rank().over(w14).alias("session_no_rank"),
        F.lead("last_known_new_group_flag", 1).over(w15).alias("next_last_known_new_group_flag"),
        F.avg("cumulative_session_no").over(w16).alias("cumulative_session_no_avg_3_prior"),
    )
)

result = (
    metrics_payments
    .groupBy(
        F.when(F.col("event_seq") >= 9, "LARGE").otherwise("SMALL").alias("event_seq_tier"),
        "session_no",
    )
    .agg(
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_rows"),
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
        F.count_if(F.col("status") == "REFUNDED").alias("refunded_events"),
        F.count_if(F.col("status") == "SUCCESS").alias("success_events"),
        F.round(F.avg("cumulative_session_no_avg_3_prior"), 2).alias("avg_cumulative_session_no_avg_3_prior"),
        F.count("*").alias("event_count"),
    )
)
