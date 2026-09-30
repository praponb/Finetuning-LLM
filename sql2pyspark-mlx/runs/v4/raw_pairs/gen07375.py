from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .groupBy(F.col("s.id"))
    .agg(F.count(F.col("we.id")).alias("web_event_count"))
    .orderBy(F.col("web_event_count").desc(), "id")
    .limit(10)
)
