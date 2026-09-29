from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left")
    .filter(F.col("web_events.session_id").isin([8, 17]))
    .groupBy(F.col("sessions.id"), F.col("sessions.browser"))
    .agg(
        F.count(F.col("web_events.id")).alias("web_event_count"),
        F.min(F.col("web_events.duration_sec")).alias("min_duration_sec"),
        F.sum(F.when(F.col("web_events.page") == "/cart", F.col("web_events.duration_sec")).otherwise(0)).alias("cart_duration_sec"),
    )
    .filter(F.col("web_event_count") >= 4)
)
