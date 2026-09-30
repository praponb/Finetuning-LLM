from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").isNotNull())
    .select("plan", "start_date", "monthly_fee")
    .orderBy("plan", F.col("monthly_fee").desc())
)
