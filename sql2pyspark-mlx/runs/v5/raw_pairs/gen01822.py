from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Starbucks", 1).otherwise(0))) / F.count("*")), 1).alias("pct_starbucks"),
        F.min("txn_time").alias("first_txn_time"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "left")
    .filter(F.col("at.pct_starbucks") > 53)
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("at.pct_starbucks"),
        F.col("at.first_txn_time"),
    )
)
