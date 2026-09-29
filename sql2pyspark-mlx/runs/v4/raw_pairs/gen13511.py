from pyspark.sql import functions as F

joined = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.browser"))
)

result = (
    joined
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "browser",
    )
    .orderBy(F.col("id").desc_nulls_first())
    .limit(10)
)
