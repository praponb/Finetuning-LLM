from pyspark.sql import functions as F

valid = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-08-05 01:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .withColumn("event_type_length", F.length("event_type"))
    .withColumn("duration_sec_whole", F.ceil("duration_sec"))
)

result = (
    valid
    .groupBy(
        "session_id",
        F.date_trunc("WEEK", "event_time").alias("week_start"),
        "page",
    )
    .agg(
        F.max("event_time").alias("last_seen"),
        F.round(F.avg("duration_sec_whole"), 2).alias("avg_duration_sec_whole"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_count"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.count_if(F.col("page") == "/cart").alias("cart_events"),
        F.percentile_approx("duration_sec_whole", 0.9).alias("duration_sec_whole_p90"),
    )
    .orderBy(F.col("cart_events").desc())
)
