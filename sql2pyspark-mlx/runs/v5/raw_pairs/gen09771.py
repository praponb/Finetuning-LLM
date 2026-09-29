from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") <= 461)
    .groupBy(
        F.when(F.col("duration_sec") >= 389, "high").when(F.col("duration_sec") >= 123, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
    .filter(F.col("search_duration_sec") >= 32)
    .select("duration_sec_bucket", "search_duration_sec")
    .orderBy("duration_sec_bucket")
    .limit(5)
)
