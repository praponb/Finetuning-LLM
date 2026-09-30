from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id", "account_type")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.sum("balance").alias("total_balance"),
    )
)
