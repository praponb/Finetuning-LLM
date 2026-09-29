from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter(F.col("duration_sec") == 340)
    .withColumn("event_time_day", F.col("event_time").cast("date"))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("duration_sec") >= 516, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)
