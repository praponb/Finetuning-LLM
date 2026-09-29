from pyspark.sql import functions as F

not_flagged = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= "2023-05-01"), "left_semi")
)

result = not_flagged.select("id", "device", "browser", "started_at", "country")
