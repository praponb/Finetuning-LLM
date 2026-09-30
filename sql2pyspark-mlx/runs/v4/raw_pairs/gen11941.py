from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2024-06-01") & F.hour("event_time").between(8, 16) & (F.col("event_time") >= "2023-05-01"))
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(F.max("event_time").alias("last_event_time"))
)
