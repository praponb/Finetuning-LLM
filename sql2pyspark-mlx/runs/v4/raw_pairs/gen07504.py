from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.signup_date") <= "2025-07-03")
    .select(
        F.col("a.customer_id"),
        F.col("a.balance"),
        F.col("c.is_active"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("credit_limit").desc())
)
