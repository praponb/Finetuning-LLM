from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNull())
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.sum(F.when(F.col("page") == "/search", 1).otherwise(0)).alias("search_count"),
    )
)
