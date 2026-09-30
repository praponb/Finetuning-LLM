from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_trunc("WEEK", "end_date").alias("week_start"))
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.count("*").alias("event_count"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
    )
)
