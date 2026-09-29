from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_time") < "2023-06-01") & (F.col("event_type") == "purchase"))
)

agg = (
    base
    .groupBy(F.year("event_time").alias("event_year"))
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
)

result = (
    agg
    .filter(F.col("total_duration_sec") < 2300)
    .orderBy(F.col("event_year").desc(), F.col("total_duration_sec").desc())
    .limit(5)
)
