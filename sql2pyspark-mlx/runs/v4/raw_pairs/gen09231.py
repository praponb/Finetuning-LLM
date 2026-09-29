from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .groupBy(F.col("s.customer_id"))
    .agg(
        F.sort_array(F.collect_set(F.col("we.event_type"))).alias("event_type_list"),
        F.round(F.avg(F.col("we.duration_sec")), 2).alias("avg_duration_sec"),
    )
)
