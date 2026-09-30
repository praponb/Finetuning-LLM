from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at_period", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("paid_at_period", "id")
w4 = Window.partitionBy("method").orderBy("paid_at_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method").orderBy("method_length", "id")
w_running = Window.partitionBy("method").orderBy("burst_id_2", "id")
w7 = Window.partitionBy("order_id").orderBy("row_seq", "id")

filtered = (
    df_payments
    .filter(F.col("method") == "bank_transfer")
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.date_trunc("year", "paid_at").alias("paid_at_period"),
        F.length("method").alias("method_length"),
    )
)

gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.lag("paid_at_period").over(w).isNull() | (F.col("paid_at_period") > F.lag("paid_at_period").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "order_id",
        "method",
        "status",
        "paid_at_period",
        "method_length",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

enriched = (
    islands
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.channel").isin(["partner", "store", "mobile"]))
    .select("s.*", F.col("d.amount"))
)

flagged = (
    enriched
    .withColumn("is_new_burst", F.when(F.col("paid_at_period") - F.lag("paid_at_period").over(w3) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands_payments = flagged.withColumn("burst_id_2", F.sum("is_new_burst").over(w4))

with_windows = (
    islands_payments
    .select(
        "*",
        F.lead("amount").over(w5).alias("next_amount"),
        F.lag("method_length").over(w5).alias("prev_method_length"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "status",
        "paid_at_period",
        "method_length",
        "amount",
        "next_amount",
        "prev_method_length",
        F.lag("burst_id_2", 1).over(w_running).alias("prev_burst_id_2"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "paid_at_period",
        "method_length",
        "amount",
        "next_amount",
        "prev_method_length",
        "prev_burst_id_2",
        "event_seq",
    )
)
