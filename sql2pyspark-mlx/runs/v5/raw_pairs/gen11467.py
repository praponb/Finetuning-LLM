from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.session_id").isin([8, 10, 18]))
    .groupBy(F.col("s.device"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.round(((100 * F.sum(F.when(F.col("we.event_type") == "page_view", 1).otherwise(0))) / F.count("*")), 1).alias("pct_page_view"),
        F.count(F.col("we.duration_sec")).alias("duration_sec_count"),
    )
    .filter(F.col("duration_sec_count") > 5)
)
