from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.customer_id").isNull() & (F.col("s.device") == "mobile"))
    .select(F.col("c.credit_limit"), F.col("c.is_active"), F.col("s.device"))
)
