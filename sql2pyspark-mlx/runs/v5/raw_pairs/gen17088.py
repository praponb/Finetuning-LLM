from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.account_type").isin(["SAVINGS", "CHECKING", "CREDIT"]))
    .select(
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("t.account_id"),
        F.col("t.txn_type"),
        F.col("t.is_flagged"),
    )
)
