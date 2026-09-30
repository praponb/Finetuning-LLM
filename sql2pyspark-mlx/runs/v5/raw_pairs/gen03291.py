from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.filter(F.col("session_id").isin([6, 15, 16, 17])).alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left_semi")
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("sessions.browser"),
    )
    .orderBy(F.col("started_at").desc())
)
