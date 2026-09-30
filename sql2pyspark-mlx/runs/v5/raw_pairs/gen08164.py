from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("txn_type").isin(["FEE", "REFUND"])).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.opened_date") <= "2025-04-05")
    .select(F.col("a.id"), F.col("a.opened_date"))
    .orderBy("id")
)
