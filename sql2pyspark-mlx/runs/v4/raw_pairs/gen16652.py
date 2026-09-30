from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.filter(F.col("country") == "US").alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left_semi")
    .filter(F.col("accounts.balance") > 62000)
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("accounts.customer_id"),
        F.col("accounts.status"),
    )
)
