from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.to_date(F.col("we.event_time")) >= "2024-07-01")
    .groupBy(F.col("s.browser"), F.year(F.col("s.started_at")).alias("started_year"))
    .agg(
        F.sort_array(F.collect_set(F.col("we.page"))).alias("page_list"),
        F.max(F.col("we.event_time")).alias("last_event_time"),
    )
)
