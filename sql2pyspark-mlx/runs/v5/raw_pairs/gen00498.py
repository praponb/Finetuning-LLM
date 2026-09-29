from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("status").isin(["OPEN", "CLOSED", "FROZEN"])).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_semi")
    .filter(F.col("t.txn_type").isin(["FEE", "REFUND", "CREDIT"]))
    .select(F.col("t.id"), F.col("t.is_flagged"))
)
