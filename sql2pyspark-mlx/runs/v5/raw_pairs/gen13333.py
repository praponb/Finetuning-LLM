from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "search"]) & (F.col("page") != "/cart"))
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
        "page",
    )
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
    )
    .orderBy("avg_duration_sec", "event_cohort", "page")
    .limit(10)
)
