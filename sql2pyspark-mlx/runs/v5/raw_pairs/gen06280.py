from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.event_type").isin(["click", "add_to_cart"]))
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(F.count(F.col("we.id")).alias("web_event_count"))
    .orderBy("web_event_count", F.col("id").desc())
)
