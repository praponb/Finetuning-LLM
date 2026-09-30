from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy("device", F.date_trunc("MONTH", "started_at").alias("activity_month"))
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.countDistinct("customer_id").alias("distinct_customer_ids_2"),
    )
    .orderBy(F.col("device").asc_nulls_first())
)
