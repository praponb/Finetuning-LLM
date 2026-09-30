from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method").orderBy(F.col("amount").desc())
w3 = Window.partitionBy("method")
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w_hist = Window.orderBy("amount", "id")
w6 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("method").orderBy("next_amount", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.partitionBy("status").orderBy("prev_paid_at", "id")
w9 = Window.partitionBy("status").orderBy("prev_paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_payments
    .withColumn("first_known_method", F.first("method", ignorenulls=True).over(w))
    .withColumn("amount_dense_rank", F.dense_rank().over(w2))
    .withColumn("method_min_amount", F.min("amount").over(w3))
    .withColumn("next_amount", F.lead("amount", 1).over(w4))
    .withColumn("row_seq", F.row_number().over(w_hist))
)

prev_events = metrics.withColumn("prev_paid_at", F.lag("paid_at").over(w4))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w6))

joined = (
    islands
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.*",
        F.col("b.discount"),
        F.col("b.channel"),
        F.col("b.amount").alias("order_amount"),
    )
)

scored = (
    joined
    .withColumn("discount_avg_6", F.avg("discount").over(w7))
    .withColumn("next_burst_id", F.lead("burst_id", 2).over(w4))
)

flagged = (
    scored
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at") - F.lag("prev_paid_at").over(w8) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("session_no", F.sum("session_start_flag").over(w9))

result = (
    sessions_payments
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        "first_known_method",
        "amount_dense_rank",
        "row_seq",
        "prev_paid_at",
    )
)
