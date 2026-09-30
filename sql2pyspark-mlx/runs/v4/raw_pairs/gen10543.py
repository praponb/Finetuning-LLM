from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("event_time") < "2023-10-03")
    .groupBy("session_id")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.started_at"),
        F.col("st.avg_duration_sec"),
    )
)
