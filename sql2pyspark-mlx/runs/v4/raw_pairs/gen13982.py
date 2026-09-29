from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.browser"), F.col("s.device"))
)
