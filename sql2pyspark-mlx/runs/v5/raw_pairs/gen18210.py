from pyspark.sql import functions as F

result = (
    df_payments
    .select("status", F.lit("payment").alias("kind"))
    .unionByName(df_subscriptions.select("status", F.lit("subscription").alias("kind")))
    .distinct()
)
