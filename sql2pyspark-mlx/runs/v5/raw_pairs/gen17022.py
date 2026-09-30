from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "click", F.col("duration_sec")).otherwise(0)).alias("click_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "left")
    .filter(F.col("st.click_duration_sec") >= 42)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.started_at"),
        F.col("st.click_duration_sec"),
    )
    .orderBy("device")
)
