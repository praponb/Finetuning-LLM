from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.countDistinct("amount").alias("unique_accounts"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(transactions_by_account.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "left")
    .select(
        F.col("accounts.account_type"),
        F.col("accounts.customer_id"),
        F.col("t.unique_accounts"),
        F.col("t.amount_count"),
    )
    .orderBy("customer_id")
)
