from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.browser"))
    .agg(
        F.min(F.col("we.duration_sec")).alias("min_duration_sec"),
        F.round(((100 * F.sum(F.when(F.col("we.event_type") == "page_view", 1).otherwise(0))) / F.count("*")), 1).alias("pct_page_view"),
    )
    .orderBy("pct_page_view", F.col("min_duration_sec").desc())
)
