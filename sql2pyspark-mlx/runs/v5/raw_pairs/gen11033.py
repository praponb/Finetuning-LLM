from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "outer")
    .filter((F.col("c.age") >= 65) | F.col("c.credit_limit").isNotNull())
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("c.signup_date"),
        F.col("c.is_active"),
    )
    .orderBy(F.col("id").desc(), "signup_date")
)
