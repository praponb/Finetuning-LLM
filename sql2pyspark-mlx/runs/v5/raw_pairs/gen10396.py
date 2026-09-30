from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.device").alias("device"), F.col("p.country").alias("country"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("a.duration_sec")).alias("total_duration_sec"),
        F.grouping_id().alias("gid"),
    )
)
