from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("avg_monthly_fee") > 240)
    .select("plan", "avg_monthly_fee", "status_list")
)
