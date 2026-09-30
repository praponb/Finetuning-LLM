from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", "customer_id")
    .pivot("status", ["FROZEN", "CLOSED"])
    .agg(F.avg("balance").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "account_type",
        "customer_id",
        F.col("FROZEN_amount").alias("frozen_amount"),
        F.col("FROZEN_n").alias("frozen_n"),
        F.col("CLOSED_amount").alias("closed_amount"),
        F.col("CLOSED_n").alias("closed_n"),
    )
)
