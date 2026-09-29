from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .filter(F.col("st.min_duration_sec") >= 470)
    .select(F.col("s.id"), F.col("s.started_at"), F.col("st.min_duration_sec"))
    .orderBy(F.col("min_duration_sec").desc())
)
