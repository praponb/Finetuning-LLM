from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .groupBy(
        F.when(F.year(F.col("s.started_at")) < 2024, "before_2024").when(F.year(F.col("s.started_at")) == 2024, "2024").otherwise("after_2024").alias("started_cohort"),
        F.col("s.country"),
    )
    .agg(
        (F.max(F.col("we.duration_sec")) - F.min(F.col("we.duration_sec"))).alias("duration_sec_range"),
    )
    .orderBy("country", "started_cohort")
)
