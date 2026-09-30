from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(F.countDistinct("amount").alias("unique_merchants"))
)

result = (
    df_accounts.alias("accounts")
    .join(transactions_by_account.alias("transactions_by_account"), F.col("accounts.id") == F.col("transactions_by_account.account_id"), "left")
    .filter(F.col("transactions_by_account.unique_merchants") > 4)
    .select(
        F.col("accounts.id"),
        F.col("accounts.account_type"),
        F.col("transactions_by_account.unique_merchants"),
    )
    .orderBy(F.col("id").desc())
)
