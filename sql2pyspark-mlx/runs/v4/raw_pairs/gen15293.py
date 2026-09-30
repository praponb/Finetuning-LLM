from pyspark.sql import functions as F

src = df_web_events.filter(F.col("duration_sec") <= 435)

summary = (
    src
    .groupBy("page", "event_type")
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.min("event_time").alias("first_event_time"),
    )
)

result = (
    summary
    .select("page", "event_type", "page_list", "first_event_time")
    .orderBy("first_event_time")
)
