from pyspark.sql import functions as F

kept_keys = (
    df_web_events
    .select("event_type", "id")
    .intersect(
        df_web_events
        .filter(~F.col("page").isin(["/product", "/checkout", "/home"]))
        .select("event_type", "id")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("duration_sec") >= 450, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
        "event_type",
    )
    .agg(
        F.max("event_time").alias("ended_at"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles_2"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("event_type") == "page_view", 1)), F.lit(0)), 2).alias("duration_sec_per_page_view"),
    )
    .filter(F.col("duration_sec_per_page_view") >= 0)
    .orderBy(F.col("duration_sec_tier").asc_nulls_last())
)
