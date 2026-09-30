from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_trunc("YEAR", "event_time").alias("year_start"))
    .agg(
        F.min("event_time").alias("first_seen"),
        F.max("event_time").alias("ended_at"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
    )
)
