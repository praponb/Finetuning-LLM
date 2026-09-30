from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "enterprise", "premium"]))
    .groupBy("customer_id")
    .agg(
        F.min("end_date").alias("first_end_date"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
    .filter(F.col("max_monthly_fee") > 150)
    .select("customer_id", "first_end_date", "max_monthly_fee")
    .orderBy(F.col("max_monthly_fee").desc())
)
