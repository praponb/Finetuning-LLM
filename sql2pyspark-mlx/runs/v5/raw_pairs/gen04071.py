from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("method").orderBy("session_start_flag", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("last4_max_session_start_flag", "id")
w5 = Window.partitionBy("order_id").orderBy("last4_sum_session_start_flag", "id")
w_grp = Window.partitionBy("order_id").orderBy("next_last4_max_session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("last4_sum_session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("order_status", "order_id").orderBy(F.col("next_last4_max_session_start_flag").desc(), "id")

filtered = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-05-27 10:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .withColumn("amount_level", F.when(F.col("amount") >= 1550, "High").when(F.col("amount") >= 410, "Medium").otherwise("Low"))
    .withColumn("adjusted_amount", F.round((F.col("amount") * 1.1), 2))
    .withColumn("method_filled", F.coalesce("method", F.lit("OTHER")))
)

valid = (
    filtered
    .filter(F.col("paid_at") > F.lit("2023-07-31").cast("date"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "method_filled",
        F.when(F.col("adjusted_amount") >= 86, "High").when(F.col("adjusted_amount") >= 16, "Medium").otherwise("Low").alias("adjusted_amount_band"),
        F.coalesce("method", F.lit("OTHER")).alias("resolved_method"),
    )
)

gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

scored = (
    sessions_payments
    .filter(F.col("amount") < 950)
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "method_filled",
        "adjusted_amount_band",
        "resolved_method",
        "session_start_flag",
        F.sum("session_start_flag").over(w_recent).alias("last4_sum_session_start_flag"),
        F.min("session_start_flag").over(w_recent).alias("last4_min_session_start_flag"),
        F.max("session_start_flag").over(w_recent).alias("last4_max_session_start_flag"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "last4_sum_session_start_flag",
        F.lead("last4_max_session_start_flag").over(w4).alias("next_last4_max_session_start_flag"),
        F.lag("last4_max_session_start_flag").over(w4).alias("prev_last4_max_session_start_flag"),
        F.lead("last4_min_session_start_flag").over(w4).alias("next_last4_min_session_start_flag"),
        F.lag("last4_min_session_start_flag").over(w4).alias("prev_last4_min_session_start_flag"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

enriched_payments = (
    enriched
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.last4_sum_session_start_flag",
        "s.next_last4_max_session_start_flag",
        "s.prev_last4_max_session_start_flag",
        "s.next_last4_min_session_start_flag",
        F.col("o.status").alias("order_status"),
    )
)

with_windows = (
    enriched_payments
    .select(
        "*",
        F.row_number().over(w5).alias("seq_no"),
        F.sum("prev_last4_max_session_start_flag").over(w_grp).alias("running_prev_last4_max_session_start_flag"),
        F.sum("next_last4_min_session_start_flag").over(w7).alias("cumulative_next_last4_min_session_start_flag"),
        F.sum("next_last4_max_session_start_flag").over(w_grp).alias("running_next_last4_max_session_start_flag"),
        F.row_number().over(w8).alias("row_seq"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        F.col("prev_last4_max_session_start_flag").alias("PREV_LAST4_MAX_SESSION_START_FLAG"),
        F.col("last4_sum_session_start_flag").alias("LAST4_SUM_SESSION_START_FLAG_VAL"),
        F.col("next_last4_max_session_start_flag").alias("NEXT_LAST4_MAX_SESSION_START_FLAG"),
    )
    .unpivot(
        ids=["id", "order_id"],
        values=["PREV_LAST4_MAX_SESSION_START_FLAG", "LAST4_SUM_SESSION_START_FLAG_VAL", "NEXT_LAST4_MAX_SESSION_START_FLAG"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
