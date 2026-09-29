from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.max("start_date").alias("last_start_date"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .orderBy(F.col("avg_monthly_fee").desc(), "plan")
    .limit(5)
)
