from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.device"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.round(((100 * F.sum(F.when(F.col("we.page") == "/search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
    .orderBy(F.col("pct_search").desc(), "device")
    .limit(3)
)
