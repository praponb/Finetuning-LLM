from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([6, 13, 17, 30]))
    .select(
        "id",
        "duration_sec",
        "session_id",
        "page",
        F.substring("page", 1, 2).alias("page_prefix"),
    )
)
