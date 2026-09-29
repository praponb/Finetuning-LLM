from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"))
    .orderBy("event_cohort", "home_count")
)
