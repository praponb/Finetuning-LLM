from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(
        "country",
        F.date_trunc("MONTH", "started_at").alias("activity_month"),
        F.date_trunc("WEEK", "started_at").alias("week_start"),
    )
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_count"),
        F.countDistinct("device").alias("distinct_devices"),
        F.min("started_at").alias("started_at"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes_2"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("span_minutes_2") < 0)
)
