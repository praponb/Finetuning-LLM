from pyspark.sql import functions as F

result = (
    df_web_events
    .groupingSets(
        [["session_id", "event_type"], ["session_id"], []],
        "session_id",
        "event_type",
    )
    .agg(
        F.sum(F.when(F.col("page") == "/home", F.col("duration_sec")).otherwise(0)).alias("home_duration_sec"),
        F.percentile_approx("duration_sec", 0.5).alias("duration_sec_p50"),
        F.max("event_time").alias("last_seen"),
        F.percentile_approx("duration_sec", 0.5).alias("duration_sec_p50_2"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("distinct_event_types").desc())
)
