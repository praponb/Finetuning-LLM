from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.device").isin(["mobile", "desktop"]))
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(F.count(F.col("we.id")).alias("web_event_count"))
    .orderBy(F.col("web_event_count").desc(), "country", "id")
    .limit(5)
)
