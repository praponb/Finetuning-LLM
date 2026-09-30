from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("event_type_lower", F.lower("event_type"))
    .withColumn("event_time_period", F.date_trunc("month", "event_time"))
    .withColumn("page_filled", F.coalesce("page", F.lit("UNKNOWN")))
)

result = (
    filtered
    .groupBy(
        F.date_trunc("WEEK", "event_time_period").alias("week_start"),
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
    )
    .agg(
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.min("event_time_period").alias("first_event_time_period"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
    )
)
