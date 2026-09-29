from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(7, 10))
    .select("status", F.lit("payment").alias("source"))
    .unionByName(
        df_subscriptions
        .select("status", F.lit("subscription").alias("source"))
    )
)
