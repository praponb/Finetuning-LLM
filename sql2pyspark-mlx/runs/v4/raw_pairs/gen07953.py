from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(268, 426) & (F.col("event_type") == "search"))
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.max("duration_sec").alias("max_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)
