from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .cube("status")
    .agg(
        F.min("prev_paid_at").alias("started_at"),
        F.percentile_approx("session_start_flag", 0.9).alias("session_start_flag_p90"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("status").asc_nulls_first())
)
