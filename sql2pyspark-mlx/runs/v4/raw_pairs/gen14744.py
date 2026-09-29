from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_time") >= "2024-03-01"))
    .select(
        "id",
        "page",
        "duration_sec",
        F.ceil("duration_sec").alias("duration_sec_whole"),
    )
    .orderBy(F.col("duration_sec_whole").desc(), "duration_sec", "id")
    .limit(20)
)
