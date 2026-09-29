from pyspark.sql import functions as F

result = (
    df_web_events
    .rollup("session_id")
    .agg(
        F.percentile_approx("duration_sec", 0.5).alias("duration_sec_p50"),
        F.min("event_time").alias("started_at"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
    )
    .orderBy("duration_sec_p50")
)
