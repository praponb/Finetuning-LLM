from pyspark.sql import functions as F

src = (
    df_web_events
    .filter(F.col("duration_sec").between(140, 192))
    .withColumn("event_type_title", F.initcap(F.trim("event_type")))
)

summary = (
    src
    .groupBy(
        F.when(F.col("duration_sec") >= 521, "high").when(F.col("duration_sec") >= 114, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.countDistinct("duration_sec").alias("unique_event_types"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

result = (
    summary
    .orderBy(F.col("unique_event_types").desc(), "duration_sec_bucket")
    .limit(10)
)
