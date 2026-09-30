from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_time") >= F.lit("2023-07-29 22:00:00").cast("timestamp")))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.hour("event_time").alias("event_time_hour"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("duration_sec") >= 444, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.avg(F.when(F.col("page") == "/search", F.col("event_time_hour"))).alias("search_avg_event_time_hour"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
    )
)
