from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Safari"]))
    .groupBy("customer_id")
    .agg(F.min("started_at").alias("min_started_at"))
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("S"), F.col("c.id") == F.col("S.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.age"), F.col("S.min_started_at"))
)
