from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .groupBy(F.col("s.id"), F.col("s.browser"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.sum(F.when(F.col("we.event_type") == "purchase", F.col("we.duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
        F.countDistinct(F.col("we.duration_sec")).alias("unique_sessions"),
    )
)
