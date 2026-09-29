from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "inner")
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.sort_array(F.collect_set(F.col("we.page"))).alias("page_list"),
        F.max(F.col("we.duration_sec")).alias("max_duration_sec"),
    )
    .orderBy("max_duration_sec")
)
