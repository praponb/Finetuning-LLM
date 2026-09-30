from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(~(F.col("a.customer_id").isin([11, 14, 33, 35]) & (F.col("a.opened_date") < "2025-07-24")))
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("c.credit_limit"),
        F.col("c.is_active"),
    )
)
