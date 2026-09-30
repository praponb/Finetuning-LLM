from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(F.col("a.account_type"))
    .agg(
        F.countDistinct(F.col("t.amount")).alias("unique_txn_types"),
        F.min(F.col("t.amount")).alias("min_amount"),
    )
    .filter(F.col("unique_txn_types") > 1)
    .orderBy("unique_txn_types")
)
