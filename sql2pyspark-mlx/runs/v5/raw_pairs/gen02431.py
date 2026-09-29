from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_first(), "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id")
w3 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("paid_at", "id")
w5 = Window.partitionBy("order_id", "status")
w6 = Window.partitionBy("method").orderBy(F.col("order_id_max_session_id").desc_nulls_first(), "id")
w_grp = Window.partitionBy("method").orderBy("session_id", "id")
w8 = Window.partitionBy("order_id").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("order_id").orderBy("row_seq_2", "id")
w_hist = Window.partitionBy("order_id").orderBy("order_id_max_session_id", "id")

ranked = (
    df_payments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_prev = ranked.withColumn("prev_paid_at", F.lag("paid_at").over(w2))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    with_gaps
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.sum("is_new_burst").over(w3).alias("session_id"),
    )
)

enriched = (
    sessions_payments
    .filter(F.col("session_id") != 6)
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("order_id_max_session_id", F.max("session_id").over(w5))
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "order_id",
        "paid_at",
        "session_id",
        "row_seq",
        "order_id_max_session_id",
        "rank_in_group",
        F.lag("session_id").over(w_grp).alias("prev_session_id"),
        F.lag("row_seq").over(w_grp).alias("prev_row_seq"),
        F.row_number().over(w_grp).alias("row_seq_2"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "order_id",
        "session_id",
        "row_seq",
        "order_id_max_session_id",
        "prev_row_seq",
        F.sum("order_id_max_session_id").over(w8).alias("running_order_id_max_session_id"),
        F.lag("prev_session_id", 1, 0).over(w9).alias("prev_prev_session_id"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "order_id",
        "order_id_max_session_id",
        "prev_row_seq",
        "running_order_id_max_session_id",
        F.lag("running_order_id_max_session_id").over(w_hist).alias("prev_running_order_id_max_session_id"),
        F.lead("row_seq").over(w_hist).alias("next_row_seq"),
        F.row_number().over(w_hist).alias("row_seq_2"),
    )
)

result = (
    metrics
    .withColumn("row_seq_2_tier", F.when(F.col("row_seq_2") >= 7, "PREMIUM").otherwise("STANDARD"))
    .rollup(
        "running_order_id_max_session_id",
        "prev_running_order_id_max_session_id",
        "row_seq_2_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("prev_row_seq", 0.5).alias("prev_row_seq_p50"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.grouping("running_order_id_max_session_id").alias("gid"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_first())
)
