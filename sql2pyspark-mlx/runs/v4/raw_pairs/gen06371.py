from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 415, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.min("event_time").alias("started_at"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles_2"),
        F.size(F.collect_set("page")).alias("num_pages"),
    )
    .orderBy("duration_sec_tier")
)
