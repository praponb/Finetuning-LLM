from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .groupBy(F.col("sessions.device"))
    .agg(
        F.max(F.col("web_events.duration_sec")).alias("max_duration_sec"),
        F.sum(F.when(F.col("web_events.page") == "/search", 1).otherwise(0)).alias("search_count"),
    )
    .filter(F.col("search_count") >= 25)
    .orderBy(F.col("device").desc())
)
