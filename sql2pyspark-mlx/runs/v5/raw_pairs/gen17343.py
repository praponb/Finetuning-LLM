from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .groupBy(F.col("a.id"), F.col("a.account_type"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.round(((100 * F.sum(F.when(F.col("t.merchant") == "Shell", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shell"),
    )
    .filter(F.col("pct_shell") > 30)
    .orderBy("account_type")
)
