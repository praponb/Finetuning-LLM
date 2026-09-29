from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_no").orderBy(F.col("promptpay_count").desc())

with_gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .groupBy("session_no", F.date_trunc("MONTH", "paid_at").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"),
        F.percentile_approx("session_no", [0.25, 0.5, 0.75]).alias("session_no_quartiles"),
        F.count("*").alias("event_count"),
    )
    .withColumn("promptpay_count_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("activity_month").asc_nulls_last())
)
