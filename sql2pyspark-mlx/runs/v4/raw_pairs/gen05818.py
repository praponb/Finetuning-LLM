from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", "session_id", F.hour("event_time").alias("event_hour"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
)
