from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.filter(F.col("started_at") < "2024-08-01").alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left_anti")
    .filter(F.col("web_events.event_type") != "add_to_cart")
    .select(
        F.col("web_events.id"),
        F.col("web_events.event_time"),
        F.col("web_events.session_id"),
    )
)
