from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.filter(F.col("device").isin(["tablet", "mobile"])).alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left_anti")
    .select(
        F.col("web_events.id"),
        F.col("web_events.session_id"),
        F.col("web_events.duration_sec"),
    )
)
