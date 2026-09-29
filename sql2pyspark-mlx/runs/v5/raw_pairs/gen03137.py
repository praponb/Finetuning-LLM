from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", "session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .filter(F.col("min_duration_sec") < 450)
    .select("event_type", "session_id", "page_values", "min_duration_sec")
    .orderBy(F.col("page_values").desc())
)
