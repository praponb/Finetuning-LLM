from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "customer_id",
        "started_at",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

result = (
    sessionized
    .cube(
        F.date_trunc("week", "started_at").alias("week_start"),
        F.date_trunc("month", "started_at").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("session_id", 0.95).alias("session_id_p95"),
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
        F.sum("session_id").alias("total_session_id"),
    )
    .orderBy(F.col("total_session_id").asc_nulls_first())
)
