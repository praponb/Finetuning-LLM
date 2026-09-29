from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 1170)
    .select("amount")
    .unionByName(
        df_subscriptions
        .filter(F.col("status") == "ACTIVE")
        .select(F.col("monthly_fee").alias("amount"))
    )
    .orderBy("amount")
)
