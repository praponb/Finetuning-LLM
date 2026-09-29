from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("event_type") == "add_to_cart").alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .filter(F.col("s.device").isin(["tablet", "mobile", "desktop"]))
    .select(F.col("s.id"), F.col("s.device"), F.col("s.browser"), F.col("s.started_at"))
)
