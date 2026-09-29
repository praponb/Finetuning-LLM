from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .select(F.col("a.id").alias("account_id"), F.col("c.id").alias("customer_id"))
)
