from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(F.col("s.started_at"), F.col("s.customer_id"), F.col("ss.total_count"))
    .orderBy(F.col("started_at").desc())
)
