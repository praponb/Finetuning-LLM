from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.min("opened_date").alias("first_opened_date"),
    )
)

result = (
    df_customers.alias("customers")
    .join(accounts_by_customer.alias("accounts_by_customer"), F.col("customers.id") == F.col("accounts_by_customer.customer_id"), "inner")
    .select(
        F.col("customers.name"),
        F.col("accounts_by_customer.last_opened_date"),
        F.col("accounts_by_customer.first_opened_date"),
    )
)
