from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .filter(F.col("sessions.browser").isNotNull())
    .groupBy(F.col("sessions.device"))
    .agg(
        F.count(F.col("web_events.id")).alias("web_event_count"),
        F.sum(F.when(F.col("web_events.event_type") == "purchase", F.col("web_events.duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
        (F.max(F.col("web_events.duration_sec")) - F.min(F.col("web_events.duration_sec"))).alias("duration_sec_range"),
    )
    .filter(F.col("duration_sec_range") >= 53)
    .orderBy(F.col("web_event_count").desc())
)
