from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("status", "order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("amount", "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")
w5 = Window.partitionBy("method").orderBy(F.col("running_amount").desc(), "id")
w6 = Window.orderBy(F.col("row_seq").desc_nulls_last())
w7 = Window.partitionBy("order_id")
w8 = Window.partitionBy("method").orderBy("paid_at", "id")
w9 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_payments
    .withColumn("next_amount", F.lead("amount", 2, 0).over(w))
    .withColumn("running_amount", F.sum("amount").over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)

latest = (
    with_windows
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

with_windows_payments = (
    latest
    .filter(F.col("next_amount").isNull())
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        "rn",
        F.row_number().over(w5).alias("seq_no"),
        F.cume_dist().over(w6).alias("row_seq_cume_dist"),
        F.max("next_amount").over(w7).alias("order_id_max_next_amount"),
    )
)

with_prev = with_windows_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w8))

with_gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w9))

remaining_keys = (
    sessions_payments
    .select("status")
    .intersect(
        df_payments
        .filter(F.col("method").isin(["card", "cash", "promptpay"]))
        .select("status")
    )
)

verified = (
    sessions_payments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "method",
        "paid_at",
        "rn",
        "seq_no",
        "row_seq_cume_dist",
        "order_id_max_next_amount",
        "session_no",
        (F.col("order_id_max_next_amount") * F.col("session_no")).alias("order_id_max_next_amount_times_session_no"),
        (F.col("session_no") + F.col("rn")).alias("session_no_plus_rn"),
    )
)

cleaned = (
    base
    .filter((F.col("paid_at") >= F.lit("2023-06-13").cast("date")) & (F.col("method").eqNullSafe(F.lit("card"))))
    .select(
        "id",
        "method",
        "paid_at",
        "rn",
        "seq_no",
        "row_seq_cume_dist",
        "order_id_max_next_amount",
        "session_no",
        "order_id_max_next_amount_times_session_no",
        (F.col("row_seq_cume_dist") * F.col("session_no_plus_rn")).alias("row_seq_cume_dist_times_session_no_plus_rn"),
    )
)

stats = (
    cleaned
    .withColumn("order_id_max_next_amount_tier", F.when(F.col("order_id_max_next_amount") >= 1236, "LARGE").otherwise("SMALL"))
    .rollup("order_id_max_next_amount_tier")
    .agg(
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("method") == "cash", 1)), F.lit(0)), 2).alias("session_no_per_cash"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.grouping("order_id_max_next_amount_tier").alias("gid"),
    )
)

result = (
    stats
    .select("order_id_max_next_amount_tier", "session_no_per_cash", "gid")
    .orderBy(F.col("order_id_max_next_amount_tier").asc_nulls_last(), "gid")
)
