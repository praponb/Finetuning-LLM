from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id", F.date_trunc("YEAR", "event_time").alias("year_start"))
    .agg(
        F.percentile_approx("duration_sec", 0.5).alias("duration_sec_p50"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count_if(F.col("event_type") == "search").alias("search_events"),
        F.min("event_time").alias("first_event_time"),
        F.count_if(F.col("page") == "/cart").alias("cart_events"),
        F.min("event_time").alias("first_seen"),
    )
    .filter(F.col("search_events") > 1)
)
