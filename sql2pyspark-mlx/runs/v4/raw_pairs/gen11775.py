from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.customer_id").isin([13, 18, 26, 27]) | (F.col("s.country") == "JP"))
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("c.credit_limit"),
        F.col("c.city"),
    )
)
