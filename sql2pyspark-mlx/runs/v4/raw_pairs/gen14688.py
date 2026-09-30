from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(F.date_trunc("month", "started_at").alias("activity_month"))
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.max("started_at").alias("last_seen"),
    )
)
