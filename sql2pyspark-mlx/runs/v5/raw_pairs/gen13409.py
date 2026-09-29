from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.upper(F.col("c.name")).like("%RAC%"))
    .select(
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("c.credit_limit"),
        F.col("c.city"),
        F.col("c.name"),
    )
)
