from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") >= "2023-12-01") & F.col("session_id").isin([24, 26, 39]))
    .groupBy("page")
    .agg(
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
    .filter(F.col("max_duration_sec") >= 170)
    .orderBy(F.col("page").desc())
)
