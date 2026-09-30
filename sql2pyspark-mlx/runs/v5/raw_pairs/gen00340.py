from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "left")
    .filter(F.col("st.avg_duration_sec") > 34)
    .select(F.col("s.customer_id"), F.col("s.started_at"), F.col("st.avg_duration_sec"))
    .orderBy("started_at")
)
