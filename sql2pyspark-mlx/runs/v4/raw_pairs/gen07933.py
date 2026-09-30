from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("c.is_active"),
        F.col("c.city"),
        F.col("c.signup_date"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
