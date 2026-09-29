from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", "customer_id")
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .orderBy(F.col("customer_id").asc_nulls_first())
)
