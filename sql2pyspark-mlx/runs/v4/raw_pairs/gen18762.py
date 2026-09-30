from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0)).alias("7_eleven_count"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0))) / F.count("*")), 1).alias("pct_7_eleven"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("tba.7_eleven_count"),
        F.col("tba.pct_7_eleven"),
    )
    .orderBy("account_type", "pct_7_eleven")
)
