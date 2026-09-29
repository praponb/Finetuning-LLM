from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("t.id"),
        F.col("t.txn_time"),
        F.col("t.txn_type"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("c.signup_date"),
    )
    .orderBy("balance", F.col("id").desc())
)
