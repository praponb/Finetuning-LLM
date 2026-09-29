from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 407, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
        F.date_trunc("WEEK", "event_time").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_rows"),
        F.sum(F.when(F.col("page") == "/home", F.col("duration_sec"))).alias("home_total_duration_sec"),
        F.max("event_time").alias("ended_at"),
    )
    .filter(F.col("home_total_duration_sec") >= 0)
)
