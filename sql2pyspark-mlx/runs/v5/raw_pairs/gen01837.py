from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("browser") != "Edge").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .select(F.col("we.id"), F.col("we.event_type"), F.col("we.page"))
    .orderBy("event_type", "id")
    .limit(10)
)
