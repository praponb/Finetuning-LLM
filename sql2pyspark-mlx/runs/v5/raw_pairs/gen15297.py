from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("dim"), F.col("s.session_id") == F.col("dim.id"), "inner")
    .rollup(F.col("dim.country").alias("country"), F.col("dim.device").alias("device"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("s.duration_sec")), 2).alias("total_duration_sec"),
    )
)
