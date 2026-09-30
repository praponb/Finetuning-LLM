from pyspark.sql import functions as F

valid = (
    df_web_events
    .filter(F.col("event_type").isin(["search", "purchase", "click"]))
    .select("*", F.length("event_type").alias("event_type_length"))
)

result = (
    valid
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .rollup(
        F.col("s.device").alias("device"),
        F.col("s.browser").alias("browser"),
        F.col("e.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("e.duration_sec")).alias("total_duration_sec"),
        F.min(F.col("e.event_type_length")).alias("min_event_type_length"),
        F.grouping_id().alias("gid"),
    )
)
