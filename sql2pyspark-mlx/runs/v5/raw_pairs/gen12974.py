from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.country").alias("country"), F.col("e.page").alias("page"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("e.duration_sec")), 2).alias("total_duration_sec"),
    )
)
