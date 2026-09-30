from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("session_id").isin([4, 6, 10, 20]))
    .withColumn("event_type_prefix", F.substring("event_type", 1, 3))
)

agg = (
    base
    .groupBy(F.month("event_time").alias("event_month"))
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)

result = (
    agg
    .orderBy(F.col("event_month").desc())
    .limit(5)
)
