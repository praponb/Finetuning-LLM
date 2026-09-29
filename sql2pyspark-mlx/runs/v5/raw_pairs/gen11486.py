from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(F.max("event_time").alias("latest_event_time"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("st.latest_event_time"),
    )
    .orderBy(F.col("customer_id").desc(), "id")
    .limit(5)
)
