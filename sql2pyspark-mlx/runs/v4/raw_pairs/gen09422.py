from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.customer_id").isin([8, 12, 13]))
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("we.duration_sec"),
        F.col("we.session_id"),
        F.col("s.customer_id"),
        F.trim(F.col("s.browser")).alias("clean_browser"),
    )
    .orderBy(F.col("customer_id").desc())
)
