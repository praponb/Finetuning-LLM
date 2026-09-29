from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("st.first_event_time"),
        F.col("st.purchase_count"),
    )
)
