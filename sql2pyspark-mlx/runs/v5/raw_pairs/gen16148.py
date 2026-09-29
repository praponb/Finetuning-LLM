from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("amount", "id")
w4 = Window.orderBy(F.col("amount").desc(), "id")
w_hist = Window.orderBy("is_refunded", "id").rowsBetween(-3, Window.currentRow)
w_running = Window.partitionBy("status").orderBy("new_group_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("is_refunded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status").orderBy("amount_bucket", "id")

base = (
    df_payments
    .filter(F.col("method").isNotNull() & (F.col("amount") < 1590))
    .select("*", F.when(F.col("method") == "card", 1).otherwise(0).alias("is_card"))
)

with_prev = base.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

scored = (
    sessions_payments
    .withColumn("next_is_card", F.lead("is_card").over(w3))
    .withColumn("prev_is_card", F.lag("is_card").over(w3))
    .withColumn("next_new_group_flag", F.lead("new_group_flag").over(w3))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("amount_bucket", F.ntile(10).over(w4))
)

filtered = (
    scored
    .filter(F.col("island_id") >= 2)
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        "prev_paid_at",
        "new_group_flag",
        "next_is_card",
        "prev_is_card",
        "seq_no",
        "amount_bucket",
        F.when(F.col("status") == "REFUNDED", 1).otherwise(0).alias("is_refunded"),
        F.coalesce("method", F.lit("NONE")).alias("method_filled"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "status",
        "new_group_flag",
        "amount_bucket",
        "method_filled",
        F.avg("prev_is_card").over(w_hist).alias("prev_is_card_avg_3"),
        F.sum("prev_is_card").over(w_running).alias("cumulative_prev_is_card"),
        F.sum("amount_bucket").over(w7).alias("running_amount_bucket"),
        F.lag("amount_bucket", 1).over(w8).alias("prev_amount_bucket"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "status",
        "new_group_flag",
        "amount_bucket",
        "method_filled",
        "prev_is_card_avg_3",
        "running_amount_bucket",
        "prev_amount_bucket",
        F.when(F.col("status").rlike("^(REFUNDED|FAILED)$"), "CORE").otherwise("OTHER_ZONE").alias("status_zone"),
    )
)
