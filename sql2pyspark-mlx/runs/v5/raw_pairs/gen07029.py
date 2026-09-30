from pyspark.sql import functions as F

valid = (
    df_web_events
    .filter((F.col("event_time") >= F.to_date(F.lit("2023-04-29"))) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.substring("page", 1, 4).alias("page_prefix"),
        F.concat_ws("-", "event_type", "page").alias("event_type_page_key"),
    )
)

result = (
    valid
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "inner")
    .select("a.*", F.col("s.browser"))
)
