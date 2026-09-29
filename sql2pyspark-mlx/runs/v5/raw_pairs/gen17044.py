from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("session_id").isin([13, 37]))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        F.when(F.col("event_type") == "purchase", 1).otherwise(0).alias("is_purchase"),
    )
)

result = (
    base
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .rollup(F.col("p.country").alias("country"), F.col("s.page").alias("page"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("s.is_purchase")), 2).alias("avg_is_purchase"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("cnt").asc_nulls_last())
)
