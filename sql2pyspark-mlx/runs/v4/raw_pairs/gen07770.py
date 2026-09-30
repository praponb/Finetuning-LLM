from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .select(
        F.col("t.amount"),
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("c.city"),
        F.col("c.country"),
    )
    .orderBy(F.col("city").desc())
)
