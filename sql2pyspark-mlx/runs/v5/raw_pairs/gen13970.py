from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["browser", "page"], ["browser"], []],
        F.col("p.browser").alias("browser"),
        F.col("src.page").alias("page"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.duration_sec")).alias("max_duration_sec"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("page").asc_nulls_first())
)
