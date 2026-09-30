from pyspark.sql import functions as F

result = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < "2025-03-12"), "left_anti")
)
