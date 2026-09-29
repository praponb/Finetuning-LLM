from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("dim"), F.col("s.session_id") == F.col("dim.id"), "left")
    .cube(F.col("dim.country").alias("country"), F.col("s.page").alias("page"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("s.duration_sec")).alias("max_duration_sec"),
        F.grouping_id().alias("gid"),
    )
)
