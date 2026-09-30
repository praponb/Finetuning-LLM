from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") > 410)
    .groupBy(
        "event_type",
        F.when(F.col("duration_sec") >= 366, "high").when(F.col("duration_sec") >= 196, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.min("duration_sec").alias("min_duration_sec"))
    .filter(F.col("min_duration_sec") < 330)
)
