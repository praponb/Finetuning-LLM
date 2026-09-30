from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") < "2024-08-01")
    .groupBy("plan")
    .agg(F.avg("monthly_fee").alias("avg_monthly_fee"))
    .filter(F.col("avg_monthly_fee") > 80)
    .orderBy(F.col("plan").desc())
)
