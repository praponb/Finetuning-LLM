from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "FROZEN") & (F.col("opened_date") <= "2023-06-24"))
    .groupBy("account_type", "status")
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
    )
    .orderBy("account_type")
)
