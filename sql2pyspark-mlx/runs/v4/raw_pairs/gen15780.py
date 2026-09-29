from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("s.plan") == "basic")
    .select(
        F.col("s.id"),
        F.col("s.monthly_fee"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
        F.col("c.email"),
    )
    .orderBy("email")
)
