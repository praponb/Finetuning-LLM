from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.country") == "DE")
    .groupBy(
        F.date_trunc("MONTH", F.col("s.started_at")).alias("started_month"),
        F.year(F.col("s.started_at")).alias("started_year"),
    )
    .agg(
        F.avg(F.col("we.duration_sec")).alias("avg_duration_sec"),
        F.min(F.col("we.duration_sec")).alias("min_duration_sec"),
    )
    .filter(F.col("min_duration_sec") > 470)
    .orderBy("avg_duration_sec")
)
