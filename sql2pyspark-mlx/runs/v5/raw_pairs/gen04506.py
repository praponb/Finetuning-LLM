from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-04-01", "2024-09-01"))
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"), "plan")
    .agg(
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee")).otherwise(0)).alias("premium_monthly_fee"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)
