from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.hour(F.col("s.started_at")).between(9, 16))
    .groupBy(
        F.quarter(F.col("s.started_at")).alias("started_quarter"),
        F.when(F.year(F.col("s.started_at")) < 2024, "before_2024").when(F.year(F.col("s.started_at")) == 2024, "2024").otherwise("after_2024").alias("started_cohort"),
    )
    .agg(F.max(F.col("we.event_time")).alias("latest_event_time"))
)
