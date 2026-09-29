from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("session_id").isin([9, 40]) & (F.col("event_time") > F.lit("2023-05-22 21:00:00").cast("timestamp")))
    .select(
        "*",
        F.concat_ws(",", "page", "event_type").alias("page_event_type_key"),
        F.coalesce("page", F.lit("NONE")).alias("page_filled"),
    )
)

result = (
    cleaned
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.session_id",
        "src.event_type",
        "src.page",
        "src.event_time",
        "src.duration_sec",
        "src.page_event_type_key",
        F.col("p.browser"),
    )
)
