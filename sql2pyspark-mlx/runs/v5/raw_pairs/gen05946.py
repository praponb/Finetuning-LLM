from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("status").orderBy("is_new_burst", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("session_id", "id")

with_gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

gaps = (
    sessions_payments
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "is_new_burst",
        "burst_id",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

enriched = (
    sessionized
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id").eqNullSafe(F.col("o.id")), "left")
    .filter(F.col("o.shipping_fee") <= 16)
    .select("s.*", F.col("o.status"))
)

scored = (
    enriched
    .select(
        "*",
        F.sum("session_id").over(w_running).alias("last7_sum_session_id"),
        F.avg("session_id").over(w_running).alias("last7_avg_session_id"),
        F.max("session_id").over(w_running).alias("last7_max_session_id"),
    )
)

scored_payments = (
    scored
    .withColumn("prev_is_new_burst", F.lag("is_new_burst").over(w4))
    .withColumn("next_is_new_burst", F.lead("is_new_burst").over(w4))
    .withColumn("next_burst_id", F.lead("burst_id").over(w4))
    .withColumn("prev_burst_id", F.lag("burst_id").over(w4))
    .withColumn("seq_no", F.row_number().over(w4))
)

result = (
    scored_payments
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .groupingSets(
        [["order_status", "payment_status"], ["order_status"], []],
        F.col("p.status").alias("order_status"),
        F.col("s.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("s.next_burst_id")).alias("avg_next_burst_id"),
    )
    .orderBy(F.col("avg_next_burst_id").desc_nulls_last())
)
