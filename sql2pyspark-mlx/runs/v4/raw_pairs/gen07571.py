from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("c.city") == "New York")
    .select(
        F.col("s.id"),
        F.col("s.start_date"),
        F.col("s.plan"),
        F.col("c.credit_limit"),
        F.col("c.is_active"),
    )
    .orderBy(F.col("id").desc())
)
