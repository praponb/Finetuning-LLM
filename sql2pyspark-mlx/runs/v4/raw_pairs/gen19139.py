from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "outer")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter((F.col("t.account_id") == 2) & (F.length(F.col("c.name")) < 9))
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("t.amount"),
        F.col("a.status"),
        F.col("c.signup_date"),
    )
    .orderBy(F.col("id").desc())
)
