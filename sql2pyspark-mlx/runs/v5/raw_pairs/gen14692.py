from pyspark.sql import functions as F

result = (
    df_payments
    .select(F.col("amount").alias("value"))
    .unionByName(df_subscriptions.select(F.col("monthly_fee").alias("value")))
    .orderBy("value")
)
