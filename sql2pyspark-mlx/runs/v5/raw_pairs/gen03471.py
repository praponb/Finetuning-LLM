from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("paid_at", "id")
w4 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method").orderBy("island_id", "id")
w6 = Window.partitionBy("method", "status").orderBy("island_id")
w7 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-11-04 05:00:00").cast("timestamp")) & (~F.col("method").eqNullSafe(F.lit("cash"))))
    .withColumn("amount_rounded", F.round("amount", 2))
    .withColumn("method_filled", F.coalesce("method", F.lit("N/A")))
)

gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_payments = (
    gaps
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        "method_filled",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

flagged = (
    sessions_payments
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w3).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w3) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        "method_filled",
        "session_no",
        F.sum("is_new_session").over(w4).alias("island_id"),
    )
)

enriched = (
    islands
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        "method_filled",
        "session_no",
        "island_id",
        F.lag("island_id", 1).over(w5).alias("prev_island_id"),
        F.percent_rank().over(w6).alias("island_id_percent_rank"),
        F.ntile(5).over(w5).alias("island_id_bucket"),
        F.sum("island_id").over(w2).alias("cumulative_island_id"),
        F.avg("session_no").over(w7).alias("session_no_avg_2"),
    )
)

result = (
    enriched
    .groupBy("method")
    .pivot("status", ["REFUNDED", "FAILED", "SUCCESS"])
    .agg(
        F.sum("island_id_percent_rank").alias("value_sum"),
        F.count(F.lit(1)).alias("cnt"),
    )
    .select(
        "method",
        F.col("REFUNDED_value_sum").alias("refunded_value_sum"),
        F.col("REFUNDED_cnt").alias("refunded_cnt"),
        F.col("FAILED_value_sum").alias("failed_value_sum"),
        F.col("FAILED_cnt").alias("failed_cnt"),
        F.col("SUCCESS_value_sum").alias("success_value_sum"),
        F.col("SUCCESS_cnt").alias("success_cnt"),
    )
)
