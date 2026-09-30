from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(194, 416))
    .select(
        "id",
        "event_time",
        F.ceil("duration_sec").alias("duration_sec_whole"),
        F.dayofmonth("event_time").alias("event_time_day"),
    )
    .orderBy(F.col("duration_sec_whole").desc(), F.col("id").desc())
    .limit(50)
)
