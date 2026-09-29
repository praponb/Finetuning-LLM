from pyspark.sql import functions as F

by_dimension = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["browser", "page"], ["browser"], []],
        F.col("d.browser").alias("browser"),
        F.col("e.page").alias("page"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("e.duration_sec")).alias("avg_duration_sec"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    by_dimension
    .select(
        "browser",
        "page",
        "cnt",
        "avg_duration_sec",
        "level_id",
        F.when(F.col("browser").rlike("^(Chrome|Firefox)$"), "CORE").when(F.col("browser").rlike("^(Safari|Edge)$"), "EXTENDED").otherwise("GROUP_A").alias("browser_zone"),
    )
)
