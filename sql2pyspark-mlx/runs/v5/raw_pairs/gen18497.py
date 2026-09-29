from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"), "event_type")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.sum(F.when(F.col("page") == "/checkout", F.col("duration_sec")).otherwise(0)).alias("checkout_duration_sec"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
    .filter(F.col("checkout_duration_sec") > 23)
)
