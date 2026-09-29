from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.plan").isin(["basic", "enterprise"]) & F.upper(F.col("c.email")).like("%ARO%"))
    .select(
        F.col("s.status"),
        F.col("s.start_date"),
        F.col("s.customer_id"),
        F.col("c.credit_limit"),
    )
)
