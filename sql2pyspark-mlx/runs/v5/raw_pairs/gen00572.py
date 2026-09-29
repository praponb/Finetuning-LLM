from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("c.credit_limit"),
        F.col("c.name"),
    )
    .orderBy(F.col("credit_limit").desc(), F.col("id").desc())
)
