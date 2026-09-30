from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .select(
        F.col("s.country"),
        F.col("c.is_active"),
        F.col("c.city"),
        F.col("c.segment"),
    )
    .orderBy("segment")
)
