from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.is_active") & F.col("c.name").isNotNull())
    .select(
        F.col("s.id"),
        F.col("s.monthly_fee"),
        F.col("s.end_date"),
        F.col("s.customer_id"),
        F.col("c.signup_date"),
        F.col("c.age"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("signup_date").desc(), F.col("end_date").desc())
)
