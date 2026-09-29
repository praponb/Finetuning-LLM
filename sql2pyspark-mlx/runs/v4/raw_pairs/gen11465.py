from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.filter(F.col("merchant").isin(["Grab", "7-Eleven"])).alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left_semi")
    .select(
        F.col("accounts.id"),
        F.col("accounts.account_type"),
        F.col("accounts.opened_date"),
        F.col("accounts.balance"),
    )
)
