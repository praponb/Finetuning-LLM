from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.filter(F.col("started_at") < "2024-12-01").alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left_anti")
    .select(F.col("web_events.id"), F.col("web_events.session_id"))
    .orderBy(F.col("session_id").desc())
)
