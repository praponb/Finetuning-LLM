from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("status").isin(["FROZEN", "CLOSED", "OPEN"])).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_anti")
    .filter(F.col("t.merchant").isin(["Starbucks", "Grab", "Lazada"]))
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("t.merchant"),
        F.col("t.is_flagged"),
    )
    .orderBy("merchant")
)
