from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isNull() & (F.col("event_time") >= "2023-03-01"))
    .select(
        "id",
        "duration_sec",
        "session_id",
        "page",
        F.initcap(F.trim("page")).alias("page_title"),
    )
    .orderBy(F.col("session_id").desc(), "id")
    .limit(5)
)
