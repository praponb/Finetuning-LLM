from pyspark.sql import functions as F

enriched = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("b"), F.col("a.session_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.device"), F.col("b.browser"))
)

result = enriched.select("id", "event_type", "page", "browser")
