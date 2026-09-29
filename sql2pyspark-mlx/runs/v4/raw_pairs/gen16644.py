from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.to_date("txn_time") >= "2023-06-01").alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .select(F.col("a.id"), F.col("a.account_type"), F.col("a.customer_id"))
    .orderBy(F.col("customer_id").desc())
)
