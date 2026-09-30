from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("start_date") >= 2024)
    .withColumn("status_prefix", F.substring("status", 1, 2))
    .withColumn("monthly_fee_rounded", F.round("monthly_fee", 1))
    .orderBy("status_prefix")
)
