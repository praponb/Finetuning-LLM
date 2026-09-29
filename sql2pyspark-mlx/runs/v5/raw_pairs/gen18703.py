from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .filter(F.col("s.browser") == "Edge")
    .groupBy(
        F.when(F.year(F.col("s.started_at")) < 2024, "before_2024").when(F.year(F.col("s.started_at")) == 2024, "2024").otherwise("after_2024").alias("started_cohort"),
    )
    .agg(
        F.max(F.col("we.duration_sec")).alias("max_duration_sec"),
        F.avg(F.col("we.duration_sec")).alias("avg_duration_sec"),
    )
    .orderBy(F.col("avg_duration_sec").desc())
)
