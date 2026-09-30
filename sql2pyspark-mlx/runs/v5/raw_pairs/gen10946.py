from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "outer")
    .filter(F.col("web_events.page").isin(["/cart", "/product", "/checkout"]) & (F.col("web_events.event_time") >= "2024-12-01"))
    .select(
        F.col("web_events.duration_sec"),
        F.col("sessions.customer_id"),
        F.col("sessions.started_at"),
        F.length(F.col("web_events.event_type")).alias("event_type_length"),
    )
    .orderBy(F.col("customer_id").desc())
)
