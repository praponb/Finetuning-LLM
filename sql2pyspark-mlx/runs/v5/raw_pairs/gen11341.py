from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .groupingSets(
        [["browser", "event_type"], ["browser"], []],
        F.col("p.browser").alias("browser"),
        F.col("s.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("s.duration_sec")), 2).alias("total_duration_sec"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("browser").asc_nulls_first())
)
