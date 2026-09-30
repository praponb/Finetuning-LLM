from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([2, 7, 22, 36]))
    .groupBy("page")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
        F.count("*").alias("num_web_events"),
        F.min("event_time").alias("first_event_time"),
    )
    .orderBy(F.col("num_web_events").desc(), "page")
)
