from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") != "enterprise") & (F.col("monthly_fee") >= 110))
    .select("monthly_fee", "plan", F.length("status").alias("status_length"))
)
