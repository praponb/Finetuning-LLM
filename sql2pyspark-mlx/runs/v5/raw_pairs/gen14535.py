from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(F.col("c.segment").isin(["SMB", "Corporate", "Consumer"]) & (F.col("c.country") == "UK"))
    .select(F.col("c.is_active"), F.col("a.customer_id"))
)
