from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .filter(F.col("accounts.customer_id") == 37)
    .select(
        F.col("accounts.opened_date"),
        F.col("accounts.account_type"),
        F.col("accounts.balance"),
        F.col("transactions.is_flagged"),
    )
    .orderBy("balance")
)
