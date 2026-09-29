from pyspark.sql import functions as F

valid = (
    df_web_events
    .filter((F.col("event_time") > F.to_timestamp(F.lit("2023-10-27 11:00:00"))) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.upper("page").alias("page_upper"),
    )
)

result = (
    valid
    .groupBy("page")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
