from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.account_type") == "CHECKING")
    .groupBy(F.date_trunc("MONTH", F.col("a.opened_date")).alias("opened_month"))
    .agg(F.min(F.col("t.txn_time")).alias("first_txn_time"))
    .orderBy("first_txn_time", "opened_month")
    .limit(5)
)
