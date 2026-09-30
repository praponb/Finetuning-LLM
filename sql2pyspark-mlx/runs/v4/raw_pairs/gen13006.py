from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.customer_id") == 14)
    .groupBy(F.col("s.id"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.min(F.col("we.event_time")).alias("min_event_time"),
    )
    .filter(F.col("web_event_count") >= 1)
    .orderBy("id")
)
