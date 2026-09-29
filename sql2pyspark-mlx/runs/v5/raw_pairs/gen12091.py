from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.countDistinct("monthly_fee").alias("unique_customers"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
)
