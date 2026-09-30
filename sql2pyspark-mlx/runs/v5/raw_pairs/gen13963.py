from pyspark.sql import functions as F

src = df_web_events.filter(F.col("session_id").isin([11, 26, 27, 39]))

event_month_summary = (
    src
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(
        F.sum(F.when(F.col("event_type") == "search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
)

result = (
    event_month_summary
    .filter(F.col("pct_search") < 27)
    .select("event_month", "search_duration_sec")
    .orderBy(F.col("search_duration_sec").desc())
)
