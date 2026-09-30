from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-06-24", "2023-11-11") | (F.col("monthly_fee") >= 90))
    .withColumn("end_date_label", F.date_format("end_date", "yyyy-MM-dd"))
)

agg = (
    recent_subscriptions
    .groupBy(F.date_trunc("MONTH", "end_date").alias("end_month"))
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
    )
)

result = agg.select("end_month", "enterprise_monthly_fee")
