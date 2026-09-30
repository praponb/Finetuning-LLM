from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("c.name").like("J%"))
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("c.email"),
        F.col("c.age"),
    )
    .orderBy("merchant")
)
