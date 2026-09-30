from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("year_start").orderBy(F.col("avg_session_start_flag").desc())

flagged = (
    df_payments
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.date_trunc("YEAR", "paid_at").alias("year_start"),
        F.when(F.col("burst_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("burst_id_tier"),
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
    )
    .agg(
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_rows"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.round(F.avg("session_start_flag"), 2).alias("avg_session_start_flag"),
    )
    .withColumn("avg_session_start_flag_percent_rank", F.percent_rank().over(w3))
)
