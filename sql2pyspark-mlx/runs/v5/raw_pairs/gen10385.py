from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.month("event_time").alias("event_month"))
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
    .filter(F.col("purchase_duration_sec") < 71)
    .select("event_month", "purchase_duration_sec", "duration_sec_range")
    .orderBy("duration_sec_range", F.col("purchase_duration_sec").desc())
)
