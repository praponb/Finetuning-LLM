from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.col("segment").isNotNull()).select("id")

result = (
    df_sessions.alias("s")
    .join(excluded_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("s.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .select(F.col("s.id"), F.col("s.country"), F.col("s.started_at"), F.col("s.device"))
)
