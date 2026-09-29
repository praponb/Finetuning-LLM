from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .groupBy(
        F.when(F.col("s.browser").isin(["Safari", "Firefox"]), "priority").otherwise("other").alias("browser_group"),
        F.date_trunc("MONTH", F.col("s.started_at")).alias("started_month"),
    )
    .agg(
        F.countDistinct(F.col("we.duration_sec")).alias("unique_pages"),
        F.sum(F.when(F.col("we.page") == "/home", F.col("we.duration_sec")).otherwise(0)).alias("home_duration_sec"),
    )
)
