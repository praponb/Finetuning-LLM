from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.filter(F.col("event_type").isin(["page_view", "click", "search"])).alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left_semi")
    .select(
        F.col("sessions.id"),
        F.col("sessions.device"),
        F.col("sessions.customer_id"),
    )
    .orderBy("id")
)
