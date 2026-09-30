from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.customer_id").isNotNull())
    .select(
        F.col("s.status"),
        F.col("s.plan"),
        F.col("s.start_date"),
        F.col("c.city"),
        F.col("c.credit_limit"),
        F.col("c.email"),
    )
)
