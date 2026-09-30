from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-07-17 17:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "*",
        F.upper("event_type").alias("event_type_upper"),
        F.when(F.col("duration_sec") >= 412, "High").when(F.col("duration_sec") >= 188, "Medium").otherwise("Low").alias("duration_sec_tier"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.event_time",
        "s.duration_sec",
        "s.duration_sec_tier",
        F.col("p.device"),
    )
)
