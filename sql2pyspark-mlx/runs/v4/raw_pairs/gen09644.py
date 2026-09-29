from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(100, 120))
    .withColumn("is_enterprise", F.when(F.col("plan") == "enterprise", 1).otherwise(0))
)

summary = (
    base
    .groupBy(F.year("end_date").alias("end_year"), "plan")
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
)

result = (
    summary
    .filter(F.col("avg_monthly_fee") < 36)
    .select("end_year", "plan", "avg_monthly_fee")
)
