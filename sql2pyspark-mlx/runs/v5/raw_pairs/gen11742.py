from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "inner")
    .rollup(
        F.col("s.device").alias("device"),
        F.col("s.browser").alias("browser"),
        F.col("a.page").alias("page"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("a.duration_sec")), 2).alias("avg_duration_sec"),
    )
    .orderBy(F.col("device").asc_nulls_last())
)
