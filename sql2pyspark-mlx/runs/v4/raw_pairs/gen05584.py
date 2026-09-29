from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city") == "London")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"), F.col("c.segment"))
    .agg(F.min(F.col("s.started_at")).alias("first_started_at"))
)
