from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .filter((F.col("s.country") == "SG") & F.col("c.is_active"))
    .select(
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("s.device"),
        F.col("c.city"),
        F.col("c.segment"),
    )
)
