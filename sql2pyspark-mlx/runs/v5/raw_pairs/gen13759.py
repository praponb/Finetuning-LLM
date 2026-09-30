from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .select(
        F.col("c.country"),
        F.col("c.age"),
        F.col("s.country").alias("session_country"),
        F.col("s.customer_id"),
        F.col("s.device"),
        F.lower(F.col("c.city")).alias("city_lower"),
    )
    .orderBy(F.col("customer_id").desc())
)
