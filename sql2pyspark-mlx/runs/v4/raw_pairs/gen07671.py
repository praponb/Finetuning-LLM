from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.age").between(33, 59))
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("s.country"),
        F.col("c.is_active"),
        F.col("c.country").alias("customer_country"),
        F.substring(F.col("s.country"), 1, 2).alias("country_prefix"),
    )
)
