from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.balance") < 37000)
    .select(
        F.col("t.merchant"),
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("c.signup_date"),
    )
    .orderBy(F.col("customer_id").desc())
)
