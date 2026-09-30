from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.event_type") == "add_to_cart")
    .groupBy(F.col("s.id"), F.col("s.device"))
    .agg(F.count(F.col("we.id")).alias("web_event_count"))
)
