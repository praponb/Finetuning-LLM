from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("activity_month"))
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_rows"),
        F.min("paid_at").alias("started_at"),
        F.round(F.sum("session_start_flag") / F.nullif(F.count(F.when(F.col("status") == "FAILED", 1)), F.lit(0)), 2).alias("session_start_flag_per_failed"),
        F.stddev_samp("amount").alias("amount_stddev_2"),
    )
    .orderBy(F.col("amount_stddev_2").asc_nulls_last())
)
