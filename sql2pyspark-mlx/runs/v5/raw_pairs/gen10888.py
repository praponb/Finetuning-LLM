from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.customer_id").isin([8, 40]) & F.col("a.account_type").isin(["SAVINGS", "CHECKING", "CREDIT"]))
    .select(
        F.col("t.id"),
        F.col("t.account_id"),
        F.col("t.txn_type"),
        F.col("t.amount"),
        F.col("a.balance"),
        F.col("a.customer_id"),
    )
)
