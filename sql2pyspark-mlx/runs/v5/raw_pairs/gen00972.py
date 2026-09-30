from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter((F.col("c.credit_limit") > 11500) & (F.year(F.col("s.start_date")) == 2024))
    .select(
        F.col("s.id"),
        F.col("s.monthly_fee"),
        F.col("c.email"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("id").desc())
)
