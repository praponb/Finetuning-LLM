from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

flagged = (
    islands
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands_payments = (
    flagged
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "burst_id",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

deduped = (
    islands_payments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

joined = (
    deduped
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.channel"), F.col("b.order_date"), F.col("b.status"))
)

kept_keys = (
    joined
    .select("order_id", "status")
    .intersectAll(
        df_payments
        .filter(F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]))
        .select("order_id", "status")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .groupBy(
        F.col("p.channel").alias("order_channel"),
        F.col("src.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.amount")).alias("max_amount"),
    )
)
