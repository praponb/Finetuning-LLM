from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 150)
    .select(F.col("monthly_fee").alias("value"))
    .intersect(df_payments.select(F.col("amount").alias("value")))
)
