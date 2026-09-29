from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "right")
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("c.email"),
        F.col("c.name"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("customer_id").desc())
)
