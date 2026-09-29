from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left")
    .select(
        F.col("sessions.customer_id"),
        F.col("web_events.event_time"),
        F.col("web_events.page"),
    )
    .orderBy(F.col("customer_id").desc(), "event_time")
)
