from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("c.credit_limit") >= 15200)
    .groupBy(
        F.col("a.customer_id"),
        F.when(F.col("a.balance") >= 62000, "high").when(F.col("a.balance") >= 17000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.sort_array(F.collect_set(F.col("t.txn_type"))).alias("txn_type_list"))
)
