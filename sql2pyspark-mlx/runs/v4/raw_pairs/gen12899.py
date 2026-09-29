from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .groupBy(F.col("a.account_type"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.countDistinct(F.col("t.amount")).alias("unique_accounts"),
    )
    .orderBy(F.col("account_type").desc())
    .limit(5)
)
