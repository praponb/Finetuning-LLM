from pyspark.sql import functions as F

dim_totals = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("dim"), F.col("a.session_id") == F.col("dim.id"), "inner")
    .groupBy(F.col("dim.device").alias("device"), F.col("a.page").alias("page"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("a.duration_sec")).alias("min_duration_sec"),
    )
)

result = (
    dim_totals
    .select("device", "page", "min_duration_sec")
    .orderBy(F.col("device").desc())
)
