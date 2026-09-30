from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.round((F.col("duration_sec") * 1.15), 2).alias("duration_sec_scaled"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_clean"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.country"))
)
