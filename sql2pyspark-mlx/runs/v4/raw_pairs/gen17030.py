from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_trunc("MONTH", "event_time").alias("activity_month"))
    .agg(
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.avg(F.when(F.col("page") == "/product", F.col("duration_sec"))).alias("product_avg_duration_sec"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
    )
    .orderBy("span_minutes")
)
