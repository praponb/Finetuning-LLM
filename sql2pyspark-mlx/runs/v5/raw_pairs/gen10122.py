from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee")
    .unionByName(df_payments.select(F.col("amount").alias("monthly_fee")))
    .distinct()
)
