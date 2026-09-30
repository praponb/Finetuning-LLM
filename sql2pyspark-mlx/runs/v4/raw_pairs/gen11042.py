from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") > "2023-05-25")
    .groupBy("status", F.date_trunc("MONTH", "start_date").alias("start_month"))
    .agg(
        F.countDistinct("monthly_fee").alias("unique_statuss"),
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("monthly_fee")).otherwise(0)).alias("active_monthly_fee"),
    )
    .filter(F.col("unique_statuss") > 2)
    .orderBy("unique_statuss")
)
