from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("a.balance") < 69000)
    .groupBy(F.col("c.country"))
    .agg(F.min(F.col("t.amount")).alias("min_amount"))
    .orderBy("country")
    .limit(1)
)
