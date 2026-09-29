from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter((F.length(F.col("c.email")) >= 7) & F.col("c.email").like("%@gmail.com"))
    .select(
        F.col("t.id"),
        F.col("t.amount"),
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("c.age"),
        F.col("c.segment"),
    )
    .orderBy("amount", "account_type")
)
