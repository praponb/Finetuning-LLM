from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2023-05-24")
    .groupBy(
        F.when(F.col("event_type").isin(["search", "click"]), "priority").otherwise("other").alias("event_type_group"),
        "event_type",
    )
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
    )
    .filter(F.col("unique_sessions") < 3)
    .orderBy("event_type_group", "event_type")
    .limit(50)
)
