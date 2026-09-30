from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("a.status") == "OPEN")
    .select(F.col("a.customer_id"), F.col("c.signup_date"))
)
