from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") != "SUCCESS")
    .select(F.col("amount").alias("value"))
    .subtract(df_subscriptions.select(F.col("monthly_fee").alias("value")))
)
