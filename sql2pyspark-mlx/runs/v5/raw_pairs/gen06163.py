from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2025-02-03") & (F.to_date("event_time") < "2025-07-26"))
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
        "page",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
    .filter(F.col("max_duration_sec") >= 260)
    .orderBy(F.col("max_duration_sec").desc())
)
