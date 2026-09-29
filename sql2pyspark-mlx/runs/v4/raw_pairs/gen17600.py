from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("event_type") == "search")
    .groupBy("session_id")
    .agg(
        F.max("event_time").alias("latest_event_time"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("st.latest_event_time"),
        F.col("st.avg_duration_sec"),
    )
    .orderBy(F.col("started_at").desc(), "latest_event_time", "id")
    .limit(5)
)
