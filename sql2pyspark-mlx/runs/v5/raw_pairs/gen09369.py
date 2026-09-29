from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .groupingSets(
        [["browser", "page"], ["browser"]],
        F.col("p.browser").alias("browser"),
        F.col("s.page").alias("page"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("s.duration_sec")), 2).alias("total_duration_sec"),
    )
    .orderBy("total_duration_sec")
)
