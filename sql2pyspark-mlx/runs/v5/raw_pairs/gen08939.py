from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.cnt") >= 7)
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("as.cnt"),
        F.col("as.shell_count"),
    )
    .orderBy(F.col("account_type").desc(), "id")
    .limit(10)
)
