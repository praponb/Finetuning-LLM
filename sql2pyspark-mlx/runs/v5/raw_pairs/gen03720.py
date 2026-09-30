from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.page").isin(["/checkout", "/search"]) | F.col("we.session_id").isin([3, 33]))
    .select(
        F.col("s.country"),
        F.col("s.device"),
        F.col("we.event_type"),
        F.col("we.event_time"),
        F.col("we.duration_sec"),
    )
    .orderBy(F.col("event_type").desc())
)
