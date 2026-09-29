from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("a.customer_id") == 30) & F.col("c.name").isNull())
    .select(F.col("a.balance"), F.col("c.name"), F.col("c.credit_limit"))
)
