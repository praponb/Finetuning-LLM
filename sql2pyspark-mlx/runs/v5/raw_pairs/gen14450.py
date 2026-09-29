from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.col("monthly_fee").alias("value"))
    .unionByName(
        df_transactions
        .filter(F.col("amount") > 1900)
        .select(F.col("amount").alias("value"))
    )
    .distinct()
)
