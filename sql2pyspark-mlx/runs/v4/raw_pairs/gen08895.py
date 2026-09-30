from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2024-04-11 19:00:00").cast("timestamp")) & (F.col("status") == "SUCCESS"))
    .withColumn("status_title", F.initcap(F.trim("status")))
    .withColumn("status_method", F.concat("status", F.lit(", "), "method"))
)

with_prev = cleaned.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_payments
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_count"),
        F.round(F.avg("session_start_flag"), 2).alias("avg_session_start_flag"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)
