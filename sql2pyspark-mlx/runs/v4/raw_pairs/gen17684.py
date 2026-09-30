from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city").isin(["Bangkok", "New York"]) & F.col("a.customer_id").isin([19, 28]))
    .select(F.col("a.status"), F.col("c.segment"))
)
