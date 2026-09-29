from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.date_trunc("MONTH", F.col("s.started_at")).alias("started_month"))
    .agg(F.round(F.avg(F.col("we.duration_sec")), 2).alias("avg_duration_sec"))
    .orderBy("started_month")
)
