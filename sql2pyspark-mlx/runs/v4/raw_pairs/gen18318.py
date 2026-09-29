from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .filter((F.col("accounts.status") == "CLOSED") & (F.col("customers.age") > 65))
    .select(F.col("accounts.customer_id"), F.col("customers.age"))
    .orderBy("customer_id")
)
