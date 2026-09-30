from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.col("monthly_fee").alias("value"))
    .subtract(df_payments.select(F.col("amount").alias("value")))
)
