from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.country"))
    .agg(
        F.sum(F.when(F.col("we.event_type") == "search", 1).otherwise(0)).alias("search_count"),
        (F.max(F.col("we.duration_sec")) - F.min(F.col("we.duration_sec"))).alias("duration_sec_range"),
    )
)
