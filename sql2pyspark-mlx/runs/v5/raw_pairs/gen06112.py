from pyspark.sql import functions as F

by_dimension = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["browser", "page"], ["browser"], []],
        F.col("d.browser").alias("browser"),
        F.col("a.page").alias("page"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("a.duration_sec")).alias("avg_duration_sec"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    by_dimension
    .select("browser", "page", "rows_total", "grouping_level")
    .orderBy(F.col("rows_total").desc_nulls_last())
)
