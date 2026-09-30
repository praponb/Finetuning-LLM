from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .filter(~(F.col("s.customer_id").isNotNull() & F.col("s.customer_id").isin([4, 28])))
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("c.age"),
        F.col("c.email"),
    )
)
