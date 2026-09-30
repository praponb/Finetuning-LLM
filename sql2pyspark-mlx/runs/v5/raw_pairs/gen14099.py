from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 28)
    .groupBy("page")
    .agg(
        F.countDistinct("duration_sec").alias("unique_pages"),
        F.sum(F.when(F.col("event_type") == "click", F.col("duration_sec")).otherwise(0)).alias("click_duration_sec"),
    )
    .filter(F.col("unique_pages") > 5)
    .orderBy("page")
    .limit(3)
)
