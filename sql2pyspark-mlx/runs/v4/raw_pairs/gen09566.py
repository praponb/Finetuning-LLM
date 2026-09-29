from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .groupBy(F.quarter(F.col("a.opened_date")).alias("opened_quarter"))
    .agg(F.count(F.when(F.col("t.txn_type") == "CREDIT", 1)).alias("credit_count"))
    .orderBy("opened_quarter")
)
