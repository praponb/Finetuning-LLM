from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "outer")
    .filter(F.col("c.country").isin(["UK", "US"]) | (F.col("c.credit_limit") >= 8300))
    .select(F.col("t.account_id"), F.col("a.balance"), F.col("c.age"), F.col("c.name"))
    .orderBy(F.col("account_id").desc())
)
