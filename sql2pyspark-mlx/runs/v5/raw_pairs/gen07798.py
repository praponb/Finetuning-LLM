from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.to_date("event_time") >= "2024-03-27")
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
        F.max("event_time").alias("latest_event_time"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_summary.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "left")
    .select(
        F.col("sessions.id"),
        F.col("sessions.customer_id"),
        F.col("sessions.browser"),
        F.col("t.purchase_duration_sec"),
        F.col("t.latest_event_time"),
    )
)
