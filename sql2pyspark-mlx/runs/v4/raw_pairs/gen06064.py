from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.status").isin(["FROZEN", "CLOSED"]))
    .groupBy(
        F.col("c.segment"),
        F.when(F.col("c.credit_limit") >= 14900, "high").when(F.col("c.credit_limit") >= 8200, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"))
)
