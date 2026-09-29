from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.to_date("event_time") < "2024-01-01").alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.browser"))
    .orderBy("browser")
)
