from pyspark.sql import functions as F

kept_keys = (
    df_web_events
    .select("id", "page")
    .intersect(
        df_web_events
        .filter(F.col("duration_sec").between(343, 388))
        .select("id", "page")
    )
)

kept_rows = (
    df_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("YEAR", "event_time").alias("year_start"))
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_rows"),
    )
    .orderBy(F.col("min_duration_sec").desc())
)
