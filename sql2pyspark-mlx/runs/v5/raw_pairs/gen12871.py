from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter(F.col("page") == "/checkout")
    .select(
        "*",
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
        F.date_trunc("YEAR", "event_time").alias("event_time_period"),
    )
)

result = (
    filtered
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "inner")
    .filter(F.col("p.started_at") >= "2024-12-01")
    .select("a.*", F.col("p.country"), F.col("p.browser"), F.col("p.device"))
)
