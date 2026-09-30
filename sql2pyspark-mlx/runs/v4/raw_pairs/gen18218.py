from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") >= "2024-11-01")
    .groupBy(
        "page",
        F.when(F.col("duration_sec") >= 380, "high").when(F.col("duration_sec") >= 113, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") < 9)
    .select("page", "duration_sec_bucket", "cnt")
)
