from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left")
    .filter(F.col("web_events.session_id").isin([1, 13, 26, 32]))
    .select(
        F.col("sessions.country"),
        F.col("sessions.browser"),
        F.col("sessions.device"),
        F.col("web_events.session_id"),
    )
)
