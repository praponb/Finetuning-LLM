from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter((F.col("c.country") != "US") | F.col("c.age").between(55, 60))
    .select(
        F.col("c.city"),
        F.col("c.country"),
        F.col("c.signup_date"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("s.device"),
    )
)
