from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.count("*").alias("web_event_count"),
    )
    .filter(F.col("web_event_count") >= 4)
    .select("event_cohort", "first_event_time", "web_event_count")
)
