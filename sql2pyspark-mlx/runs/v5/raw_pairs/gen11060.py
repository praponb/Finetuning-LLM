from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "inner")
    .select(
        F.col("accounts.status"),
        F.col("customers.city"),
        F.col("customers.credit_limit"),
        F.col("customers.signup_date"),
        F.coalesce(F.col("customers.name"), F.lit("unassigned")).alias("name_filled"),
    )
)
