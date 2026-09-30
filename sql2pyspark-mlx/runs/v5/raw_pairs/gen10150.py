from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "left")
    .filter(~((F.col("accounts.customer_id") == 31) & F.col("accounts.status").isin(["OPEN", "CLOSED", "FROZEN"])))
    .select(F.col("accounts.id"), F.col("accounts.status"), F.col("customers.email"))
    .orderBy("status")
)
