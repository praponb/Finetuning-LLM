from pyspark.sql import functions as F

kept_keys = (
    df_web_events
    .select("event_type", "id")
    .exceptAll(
        df_web_events
        .filter(F.col("page") == "/checkout")
        .select("event_type", "id")
    )
)

reconciled = (
    df_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("duration_sec") >= 451, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("event_type") == "page_view", 1)), F.lit(0)), 2).alias("duration_sec_per_page_view"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
    )
    .orderBy(F.col("duration_sec_per_page_view").asc_nulls_last())
)
