from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("activity_month", F.date_trunc("month", "started_at"))
    .cube("activity_month", "country")
    .agg(
        F.count_if(F.col("browser") == "Safari").alias("safari_events"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.grouping_id().alias("grouping_level"),
    )
)
