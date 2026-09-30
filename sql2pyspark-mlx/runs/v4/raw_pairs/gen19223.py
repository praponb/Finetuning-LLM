from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .filter((F.col("sessions.country") == "DE") | F.col("sessions.country").isin(["SG", "DE"]))
    .select(
        F.col("web_events.duration_sec"),
        F.col("sessions.started_at"),
        F.coalesce(F.col("sessions.browser"), F.lit("unassigned")).alias("browser_filled"),
    )
)
