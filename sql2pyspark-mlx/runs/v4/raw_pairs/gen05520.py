from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") >= "2023-09-01") & F.col("plan").isin(["standard", "basic", "premium"]))
    .select("id", "monthly_fee", "start_date")
    .orderBy("id", F.col("monthly_fee").desc())
)
