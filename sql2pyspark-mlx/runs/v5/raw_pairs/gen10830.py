from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "inner")
    .groupingSets(
        [["browser", "country"], ["browser"], []],
        F.col("s.browser").alias("browser"),
        F.col("s.country").alias("country"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.duration_sec")).alias("max_duration_sec"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("cnt").desc_nulls_last())
)
