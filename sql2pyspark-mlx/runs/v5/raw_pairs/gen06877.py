from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.device").alias("device"), F.col("d.browser").alias("browser"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.duration_sec")).alias("max_duration_sec"),
    )
    .orderBy(F.col("rows_total").desc_nulls_first())
)
