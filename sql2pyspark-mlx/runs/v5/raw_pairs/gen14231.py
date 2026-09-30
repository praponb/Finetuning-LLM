from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(F.min("event_time").alias("first_event_time"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .select(F.col("s.customer_id"), F.col("s.country"), F.col("st.first_event_time"))
)
