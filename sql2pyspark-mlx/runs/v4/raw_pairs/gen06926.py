from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("year_start", F.date_trunc("YEAR", "event_time"))
    .groupingSets(
        [["event_type", "year_start"], ["event_type"], []],
        "event_type",
        "year_start",
    )
    .agg(
        F.countDistinct("page").alias("distinct_pages"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("event_type").asc_nulls_last())
)
