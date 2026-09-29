from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .groupBy(F.col("s.id"), F.col("s.browser"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.count(F.col("we.duration_sec")).alias("duration_sec_count"),
        F.sum(F.when(F.col("we.event_type") == "click", 1).otherwise(0)).alias("click_count"),
    )
    .filter(F.col("duration_sec_count") > 3)
    .orderBy("web_event_count")
)
