from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .filter(F.col("accounts.status") == "OPEN")
    .groupBy(F.col("accounts.id"), F.col("accounts.status"))
    .agg(
        F.count(F.col("transactions.id")).alias("transaction_count"),
        (F.max(F.col("transactions.amount")) - F.min(F.col("transactions.amount"))).alias("amount_range"),
    )
)
