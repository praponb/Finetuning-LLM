from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([4, 18]))
    .groupBy(
        "page",
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0)).alias("checkout_count"),
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
    )
    .filter(F.col("purchase_duration_sec") >= 24)
    .drop("purchase_duration_sec")
    .orderBy("page")
)
