from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(~F.col("s.device").isin(["tablet", "desktop"]))
    .groupBy(F.col("s.id"), F.col("s.browser"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.sum(F.when(F.col("we.page") == "/search", F.col("we.duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
)
