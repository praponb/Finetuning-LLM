from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.date_trunc("MONTH", "event_time").alias("event_month"),
        "event_type",
        "page",
    )
    .agg(
        F.sum(F.when(F.col("page") == "/home", F.col("duration_sec")).otherwise(0)).alias("home_duration_sec"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_count"),
    )
    .filter(F.col("home_duration_sec") > 35)
    .orderBy(F.col("search_count").desc())
)
