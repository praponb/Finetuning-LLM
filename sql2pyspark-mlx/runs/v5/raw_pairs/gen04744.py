from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("dim"), F.col("s.session_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.country").alias("country"),
        F.col("dim.browser").alias("browser"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("s.duration_sec")).alias("min_duration_sec"),
    )
)
