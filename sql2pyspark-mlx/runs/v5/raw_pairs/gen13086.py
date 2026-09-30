from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .groupBy(F.col("s.browser"))
    .agg(
        F.max(F.col("we.duration_sec")).alias("max_duration_sec"),
        F.round(F.avg(F.col("we.duration_sec")), 2).alias("avg_duration_sec"),
    )
    .orderBy("avg_duration_sec", F.col("max_duration_sec").desc())
)
