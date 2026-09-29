from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    flagged
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessions_payments
    .groupBy("status", F.date_trunc("QUARTER", "paid_at").alias("quarter_start"))
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.percentile_approx("island_id", 0.95).alias("island_id_p95"),
    )
    .orderBy(F.col("quarter_start").desc())
)
