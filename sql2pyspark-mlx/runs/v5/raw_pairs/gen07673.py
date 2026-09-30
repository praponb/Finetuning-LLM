from pyspark.sql import functions as F

by_dimension = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("dim"), F.col("e.session_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["country", "event_type"], ["country"], []],
        F.col("dim.country").alias("country"),
        F.col("e.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.duration_sec")).alias("max_duration_sec"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    by_dimension
    .select(
        "country",
        "event_type",
        "num_rows",
        "max_duration_sec",
        "level_id",
        F.when(F.col("country").rlike("^(TH|US)$"), "PRIORITY").otherwise("GROUP_A").alias("country_zone"),
    )
    .orderBy("max_duration_sec")
)
