from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("merchant") != "7-Eleven")
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Shell", F.col("amount")).otherwise(0)).alias("shell_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.customer_id"), F.col("tba.shell_amount"))
)
