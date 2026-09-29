from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.browser"), F.col("p.device"))
)
