from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "outer")
    .filter((F.col("t.amount") >= 4000) & (F.col("t.merchant") == "Grab"))
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("t.txn_type"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.when(F.col("a.account_type") == "LOAN", 1).otherwise(0).alias("is_loan"),
    )
    .orderBy(F.col("id").desc())
)
