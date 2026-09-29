from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_accounts.filter(F.col("balance").between(26000, 53000)).alias("accounts"), F.col("customers.id") == F.col("accounts.customer_id"), "left_semi")
    .select(
        F.col("customers.id"),
        F.col("customers.is_active"),
        F.col("customers.credit_limit"),
        F.col("customers.email"),
    )
    .orderBy("credit_limit", "id")
    .limit(1)
)
