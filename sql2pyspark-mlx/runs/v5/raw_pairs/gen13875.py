from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(F.col("c.credit_limit") <= 5300)
    .select(F.col("c.age"), F.col("c.city"), F.col("a.customer_id"))
)
