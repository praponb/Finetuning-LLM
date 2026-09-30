from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.col("session_id") == 17) & (F.col("duration_sec") > 197)) | (F.col("page") == "/search"))
    .groupBy("event_type", F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
    .orderBy(F.col("pct_search").desc())
)
