from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.balance") >= 48000)
    .select(F.col("a.id"), F.col("a.account_type"), F.col("c.email"), F.col("c.city"))
    .orderBy(F.col("id").desc())
    .limit(1)
)
