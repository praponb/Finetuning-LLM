from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        F.count("*").alias("event_count"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
    )
)
