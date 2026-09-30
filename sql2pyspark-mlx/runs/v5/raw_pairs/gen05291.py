from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.sum(F.when(F.col("account_type") == "LOAN", F.col("balance")).otherwise(0)).alias("loan_balance"),
    )
)
