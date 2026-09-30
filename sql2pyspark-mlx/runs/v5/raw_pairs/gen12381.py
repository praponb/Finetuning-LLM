from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 2)
    .select(
        "id",
        "event_type",
        "page",
        F.floor("duration_sec").alias("duration_sec_whole"),
        F.ceil("duration_sec").alias("duration_sec_whole_2"),
    )
    .orderBy("page", F.col("duration_sec_whole_2").desc())
)
