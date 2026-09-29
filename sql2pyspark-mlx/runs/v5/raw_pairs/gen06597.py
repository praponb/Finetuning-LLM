from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/product", F.col("duration_sec")).otherwise(0)).alias("product_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "inner")
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("t.product_duration_sec"),
        F.col("t.total_duration_sec"),
    )
)
