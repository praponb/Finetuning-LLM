from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("t.is_flagged"))
    .groupBy(F.col("a.customer_id"))
    .agg(F.sort_array(F.collect_set(F.col("t.txn_type"))).alias("txn_type_list"))
    .orderBy(F.col("customer_id").desc())
    .limit(50)
)
