from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.age").isNotNull())
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.col("c.age"),
        F.col("c.email"),
        F.col("c.signup_date"),
    )
    .orderBy("status", "signup_date")
)
