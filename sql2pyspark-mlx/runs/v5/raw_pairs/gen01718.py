from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("amount").asc_nulls_last())
w2 = Window.partitionBy("status").orderBy(F.col("paid_at").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w4 = Window.partitionBy("method").orderBy("amount_cume_dist", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("method").orderBy("paid_at", "id")
w6 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("method").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_payments
    .select(
        "*",
        F.cume_dist().over(w_grp).alias("amount_cume_dist"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

metrics_payments = (
    metrics
    .withColumn("last7_count_amount", F.count("amount").over(w4))
    .withColumn("last7_max_amount", F.max("amount").over(w4))
    .withColumn("last7_min_amount", F.min("amount").over(w4))
    .withColumn("last7_avg_amount", F.avg("amount").over(w4))
)

gaps = (
    metrics_payments
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w5).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w5) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "amount_cume_dist",
        "event_seq",
        "row_seq",
        "last7_count_amount",
        "last7_max_amount",
        "last7_avg_amount",
        F.sum("new_group_flag").over(w6).alias("session_no"),
    )
)

gaps_2 = (
    islands
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w5).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w5) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps_2
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "row_seq",
        "last7_avg_amount",
        "session_no",
        F.sum("is_new_burst").over(w6).alias("session_no_2"),
    )
)

flagged = (
    sessionized
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w5).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w5) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands_payments = flagged.withColumn("session_no_3", F.sum("is_new_session").over(w6))

with_windows = (
    islands_payments
    .select(
        "id",
        "amount",
        "paid_at",
        "last7_avg_amount",
        "session_no_2",
        "is_new_session",
        "session_no_3",
        F.avg("session_no").over(w_ordered).alias("running_avg_session_no"),
        F.count("session_no").over(w_ordered).alias("running_count_session_no"),
        F.sum("session_no").over(w_ordered).alias("running_sum_session_no"),
    )
)

filtered = (
    with_windows
    .filter(F.to_date("paid_at") < "2023-10-01")
    .select(
        "*",
        F.round((F.col("is_new_session") * 0.9), 2).alias("adjusted_is_new_session"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("running_count_session_no") >= 15, "LARGE").otherwise("SMALL").alias("running_count_session_no_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
