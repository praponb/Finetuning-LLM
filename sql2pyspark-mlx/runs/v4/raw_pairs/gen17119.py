from pyspark.sql import functions as F

with_dims = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.browser"), F.col("d.country"), F.col("d.device"))
)

result = (
    with_dims
    .select("id", "page", "event_time", "duration_sec", "country")
    .orderBy(F.col("event_time").desc())
)
