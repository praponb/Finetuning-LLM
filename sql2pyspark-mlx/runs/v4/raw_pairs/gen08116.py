from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.browser"), F.col("b.device"), F.col("b.country"))
)
