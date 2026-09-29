from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("order_id", "status").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("next_amount", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy(F.col("prev_amount").desc(), "id")
w4 = Window.partitionBy("order_id").orderBy("seq_no", "id")
w5 = Window.partitionBy("method").orderBy("paid_at", "id")
w6 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("method", "order_id").orderBy("next_amount_avg_3", "id")
w8 = Window.partitionBy("order_id", "method").orderBy("next_amount", "id")
w9 = Window.partitionBy("method").orderBy("next_amount", "id")
w10 = Window.partitionBy("order_id").orderBy("seq_no", "id").rowsBetween(-2, -1)

with_windows = (
    df_payments
    .filter(F.to_date("paid_at") >= "2023-10-01")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.lag("amount").over(w_part).alias("prev_amount"),
        F.lead("amount").over(w_part).alias("next_amount"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.avg("next_amount").over(w2).alias("next_amount_avg_3"),
        F.row_number().over(w3).alias("row_seq"),
        F.ntile(10).over(w4).alias("seq_no_bucket"),
    )
)

flagged = (
    metrics
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w5).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w5) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("session_start_flag").over(w6))

with_windows_payments = (
    islands
    .select(
        "*",
        F.lead("next_amount").over(w7).alias("next_next_amount"),
        F.lag("next_amount").over(w7).alias("prev_next_amount"),
        F.lead("next_amount_avg_3").over(w7).alias("next_next_amount_avg_3"),
        F.row_number().over(w7).alias("seq_no_2"),
        F.ntile(4).over(w8).alias("next_amount_bucket"),
    )
)

with_windows_payments_2 = (
    with_windows_payments
    .select(
        "id",
        "prev_amount",
        "next_amount",
        "next_amount_avg_3",
        "row_seq",
        "session_start_flag",
        "island_id",
        "next_next_amount",
        "seq_no_2",
        "next_amount_bucket",
        F.lag("next_next_amount_avg_3").over(w9).alias("prev_next_next_amount_avg_3"),
        F.lag("next_amount").over(w9).alias("prev_next_amount_2"),
        F.row_number().over(w9).alias("row_seq_2"),
        F.avg("session_start_flag").over(w10).alias("session_start_flag_avg_2_prior"),
    )
)

result = (
    with_windows_payments_2
    .rollup(
        F.when(F.col("prev_next_next_amount_avg_3") >= 1351, "HIGH").otherwise("LOW").alias("prev_next_next_amount_avg_3_tier"),
        F.when(F.col("prev_next_amount_2") >= 1430, "LARGE").otherwise("SMALL").alias("prev_next_amount_2_tier"),
    )
    .agg(
        F.percentile_approx("next_amount", 0.95).alias("next_amount_p95"),
        F.round(F.avg("prev_next_next_amount_avg_3"), 2).alias("avg_prev_next_next_amount_avg_3"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.sum("row_seq").alias("total_row_seq"),
    )
    .filter(F.col("avg_prev_next_next_amount_avg_3") >= 0)
)
