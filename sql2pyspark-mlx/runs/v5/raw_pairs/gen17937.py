from pyspark.sql import functions as F

valid = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        F.quarter("event_time").alias("event_time_quarter"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("resolved_page"),
    )
)

result = (
    valid
    .groupBy(F.date_trunc("week", "event_time").alias("week_start"))
    .agg(
        F.countDistinct("page").alias("distinct_pages"),
        F.sum(F.when(F.col("page") == "/search", F.col("event_time_quarter"))).alias("search_total_event_time_quarter"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
    )
    .filter(F.col("duration_min") > 0)
    .orderBy("duration_min")
)
