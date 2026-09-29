from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.year(F.col("s.started_at")) < 2024, "before_2024").when(F.year(F.col("s.started_at")) == 2024, "2024").otherwise("after_2024").alias("started_cohort"),
        F.col("s.device"),
    )
    .agg(
        F.count(F.when(F.col("we.event_type") == "purchase", 1)).alias("purchase_count"),
        F.count("*").alias("row_count"),
    )
)
