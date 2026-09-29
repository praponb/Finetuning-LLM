from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("method").orderBy("paid_at", "id")
w3 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w5 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id")
w7 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.partitionBy("status", "method").orderBy(F.col("prev_paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w9 = Window.orderBy(F.col("prev_paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

latest = (
    df_payments
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

prev_events = latest.withColumn("prev_paid_at", F.lag("paid_at").over(w2))

with_gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w3))

flagged = (
    sessionized
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w4) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized_payments = flagged.withColumn("session_no", F.sum("is_new_session").over(w5))

valid = (
    sessionized_payments
    .filter(F.to_date("paid_at") < "2024-11-01")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "prev_paid_at",
        "island_id",
        "is_new_session",
        "session_no",
        F.datediff(F.current_date(), "prev_paid_at").alias("days_since_prev_paid_at"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_clean"),
    )
)

result = (
    valid
    .select(
        "*",
        F.avg("days_since_prev_paid_at").over(w6).alias("order_id_avg_days_since_prev_paid_at"),
        F.avg("days_since_prev_paid_at").over(w7).alias("days_since_prev_paid_at_avg_5"),
        F.sum("amount").over(w8).alias("amount_sum_90d"),
        F.max("amount").over(w9).alias("amount_max_90d"),
    )
)
