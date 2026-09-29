from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("customer_id").isin([9, 26, 32])).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_anti")
    .filter(F.hour(F.col("t.txn_time")).between(11, 17))
    .select(F.col("t.id"), F.col("t.amount"))
)
