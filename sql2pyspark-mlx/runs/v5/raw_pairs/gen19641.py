from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    with_gaps
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

result = (
    sessions_payments
    .groupBy(
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
        F.when(F.col("amount") >= 1640, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.avg("session_id").alias("avg_session_id"),
        F.max("paid_at").alias("last_paid_at"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("session_id"))).alias("failed_total_session_id"),
    )
    .orderBy(F.col("failed_total_session_id").asc_nulls_last())
)
