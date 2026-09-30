from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("c.city").isin(["Tokyo", "London"]))
    .groupBy(F.col("c.country"))
    .agg(F.min(F.col("t.txn_time")).alias("min_txn_time"))
    .orderBy("country")
)
