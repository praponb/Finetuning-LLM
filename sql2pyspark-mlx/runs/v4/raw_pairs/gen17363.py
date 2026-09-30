from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.date_trunc("MONTH", "event_time").alias("event_month"),
        F.when(F.col("duration_sec") >= 486, "high").when(F.col("duration_sec") >= 64, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.count("duration_sec").alias("duration_sec_count"),
        F.max("event_time").alias("latest_event_time"),
    )
)
