from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("duration_sec").between(263, 446))
    .groupBy("session_id")
    .agg(F.countDistinct("duration_sec").alias("unique_pages"))
)

result = (
    df_sessions.alias("sessions")
    .join(session_totals.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "left")
    .select(F.col("sessions.browser"), F.col("t.unique_pages"))
)
