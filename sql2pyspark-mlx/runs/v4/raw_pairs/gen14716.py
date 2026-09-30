from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["PAUSED", "ACTIVE", "EXPIRED"]))
    .groupBy("status", "plan")
    .agg(F.countDistinct("monthly_fee").alias("unique_plans"))
    .filter(F.col("unique_plans") >= 5)
)
