from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.device").alias("device"), F.col("e.page").alias("page"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("e.duration_sec")).alias("min_duration_sec"),
    )
)
