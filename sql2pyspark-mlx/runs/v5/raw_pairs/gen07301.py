from pyspark.sql import functions as F

dim_totals = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "inner")
    .groupingSets(
        [["browser", "page"], ["browser"]],
        F.col("s.browser").alias("browser"),
        F.col("a.page").alias("page"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("a.duration_sec")), 2).alias("total_duration_sec"),
    )
)

result = (
    dim_totals
    .select("total_duration_sec")
    .orderBy(F.col("total_duration_sec").desc())
)
