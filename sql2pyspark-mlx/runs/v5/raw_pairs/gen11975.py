from pyspark.sql import functions as F

rollup_totals = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("dim"), F.col("src.session_id") == F.col("dim.id"), "inner")
    .cube(F.col("dim.device").alias("device"), F.col("src.page").alias("page"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("src.duration_sec")), 2).alias("total_duration_sec"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    rollup_totals
    .select(
        "device",
        "cnt",
        "total_duration_sec",
        "level_id",
        F.when(F.col("device").rlike("^(mobile|tablet)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("device_zone"),
    )
    .orderBy(F.col("device_zone").asc_nulls_last())
)
