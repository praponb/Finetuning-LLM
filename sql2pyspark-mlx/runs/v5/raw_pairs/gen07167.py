from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-07-15 14:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.round("duration_sec", 0).alias("duration_sec_rounded"),
        F.coalesce("page", F.lit("NONE")).alias("page_filled"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .cube(
        F.col("p.country").alias("country"),
        F.col("p.device").alias("device"),
        F.col("s.page").alias("page"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("s.duration_sec")).alias("max_duration_sec"),
    )
)
