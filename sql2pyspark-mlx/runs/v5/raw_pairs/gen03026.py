from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_trunc("MONTH", "start_date").alias("activity_month"))
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
        F.countDistinct("status").alias("distinct_status"),
    )
)
