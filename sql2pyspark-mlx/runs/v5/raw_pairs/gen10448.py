from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .select(
        F.col("transactions.id"),
        F.col("transactions.account_id"),
        F.col("accounts.account_type"),
        F.when(F.col("accounts.account_type").isin(["SAVINGS", "CREDIT"]), "Group A").otherwise("Other").alias("account_type_group"),
    )
    .orderBy(F.col("account_id").desc())
)
