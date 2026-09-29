from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount_int", "id")
w_running = Window.partitionBy("order_id").orderBy(F.col("amount_int").desc(), "id")
w_recent = Window.partitionBy("method").orderBy("amount_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w6 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2024-01-15 18:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("method", F.lit("OTHER")).alias("method_filled"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w_running).alias("seq_no"),
        F.sum("amount_int").over(w_recent).alias("running_amount_int"),
        F.avg("amount_int").over(w_part).alias("amount_int_avg_3"),
    )
)

with_gaps = (
    with_windows
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w5).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w5) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w6))

result = (
    islands
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        "amount_int",
        "method_filled",
        "event_seq",
        "seq_no",
        "running_amount_int",
    )
    .orderBy("method")
)
