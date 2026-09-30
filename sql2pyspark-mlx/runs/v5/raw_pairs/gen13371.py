from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.max("balance").alias("max_balance"),
        F.sum("balance").alias("total_balance"),
    )
)
