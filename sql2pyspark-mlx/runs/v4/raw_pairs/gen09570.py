from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("a.status").isin(["CLOSED", "OPEN"]))
    .groupBy(
        F.col("a.account_type"),
        F.when(F.col("a.balance") >= 61000, "high").when(F.col("a.balance") >= 28000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.sum(F.col("t.amount")).alias("total_amount"),
        F.min(F.col("t.amount")).alias("min_amount"),
    )
)
