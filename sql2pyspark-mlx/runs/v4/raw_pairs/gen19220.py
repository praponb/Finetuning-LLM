from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("a.opened_date") > "2025-03-17") & (F.col("t.amount") > 3900))
    .select(
        F.col("t.merchant"),
        F.col("t.account_id"),
        F.col("a.balance"),
        F.col("c.is_active"),
        F.col("c.age"),
    )
    .orderBy(F.col("account_id").desc(), F.col("age").desc())
)
