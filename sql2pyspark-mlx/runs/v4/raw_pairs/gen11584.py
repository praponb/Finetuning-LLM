from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([2, 9, 33, 38]))
    .groupBy("event_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .orderBy("event_type")
)
