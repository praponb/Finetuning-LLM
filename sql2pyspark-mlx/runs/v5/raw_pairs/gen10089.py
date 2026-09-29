from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "right")
    .groupBy(
        F.when(F.col("sessions.browser").isin(["Safari", "Edge"]), "group_a").otherwise("other").alias("browser_group"),
    )
    .agg(
        F.avg(F.col("web_events.duration_sec")).alias("avg_duration_sec"),
        F.sum(F.col("web_events.duration_sec")).alias("total_duration_sec"),
    )
    .orderBy("browser_group", F.col("avg_duration_sec").desc())
)
