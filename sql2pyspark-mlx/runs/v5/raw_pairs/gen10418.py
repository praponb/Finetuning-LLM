from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "left")
    .select(
        F.col("s.device"),
        F.col("s.started_at"),
        F.col("st.total_duration_sec"),
        F.col("st.avg_duration_sec"),
    )
    .orderBy("avg_duration_sec", "total_duration_sec")
)
