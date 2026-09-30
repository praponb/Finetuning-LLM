from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("session_id") == 27)
    .groupBy("session_id")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .select(F.col("s.country"), F.col("s.browser"), F.col("st.total_duration_sec"))
)
